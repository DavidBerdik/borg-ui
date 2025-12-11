"""
Mount service - handles mounting and unmounting Borg repositories and archives
"""
import asyncio
import os
import json
from datetime import datetime
from pathlib import Path
import structlog
from sqlalchemy.orm import Session
from app.database.models import MountJob, Repository
from app.database.database import SessionLocal
from app.config import settings
from app.core.borg import borg

logger = structlog.get_logger()

def get_process_start_time(pid: int) -> int:
    """
    Read process start time from /proc/[pid]/stat
    Returns: jiffies since system boot (unique per process)
    """
    try:
        with open(f'/proc/{pid}/stat', 'r') as f:
            stat_data = f.read()
        # Parse: pid (comm) state ppid ... starttime (22nd field)
        fields = stat_data.split(')')[1].split()
        starttime = int(fields[19])  # 22nd field overall
        return starttime
    except Exception as e:
        logger.error("Failed to read process start time", pid=pid, error=str(e))
        return 0

class MountService:
    """Service for managing Borg mount operations"""

    def __init__(self):
        self.mount_base_dir = Path("/mnt/borg")
        self.mount_base_dir.mkdir(parents=True, exist_ok=True)
        self.running_mounts = {}  # Track running mount processes by job_id

    def _generate_mount_point(self, repository_id: int, archive_name: str = None, custom_path: str = None) -> str:
        """Generate mount point path"""
        if custom_path:
            # Validate custom path is within mount base directory for security
            custom_path_str = str(custom_path).strip()
            if not custom_path_str.startswith(str(self.mount_base_dir)):
                raise ValueError(
                    f"Custom mount point must be within {self.mount_base_dir} for security. "
                    f"Provided path: {custom_path_str}"
                )
            # Normalize path
            custom_path = Path(custom_path_str)
            # Ensure it's an absolute path
            if not custom_path.is_absolute():
                raise ValueError(f"Mount point must be an absolute path: {custom_path_str}")
            return str(custom_path.resolve())
        
        if archive_name:
            # Sanitize archive name for filesystem safety
            safe_archive_name = archive_name.replace('/', '_').replace('\\', '_')
            return str(self.mount_base_dir / f"{repository_id}-{safe_archive_name}")
        else:
            return str(self.mount_base_dir / str(repository_id))

    async def mount_repository(
        self,
        repository_id: int,
        archive_name: str = None,
        mount_point: str = None,
        mount_options: list = None,
        foreground: bool = False,
        consider_checkpoints: bool = False,
        paths: str = "",
        strip_components: str = "",
        glob_archives: str = "",
        first_n: str = "",
        last_n: str = "",
        db: Session = None
    ) -> MountJob:
        """
        Mount a repository or archive
        
        Args:
            repository_id: Repository ID
            archive_name: Optional archive name (if None, mounts entire repository)
            mount_point: Optional custom mount point path
            mount_options: List of mount options
            db: Database session
            
        Returns:
            MountJob instance
        """
        if db is None:
            db = SessionLocal()

        try:
            # Get repository
            repository = db.query(Repository).filter(Repository.id == repository_id).first()
            if not repository:
                raise ValueError(f"Repository not found (ID: {repository_id})")

            # Check if already mounted
            existing_mount = db.query(MountJob).filter(
                MountJob.repository_id == repository_id,
                MountJob.archive_name == archive_name,
                MountJob.status == "mounted"
            ).first()

            if existing_mount:
                raise ValueError(f"Repository/archive already mounted at {existing_mount.mount_point}")

            # Generate mount point
            custom_mount_point = mount_point is not None
            mount_point_path = self._generate_mount_point(repository_id, archive_name, mount_point)

            # Ensure mount point directory exists
            mount_path = Path(mount_point_path)
            mount_path.mkdir(parents=True, exist_ok=True)

            # Build full mount options dict for storage
            mount_options_dict = {
                "options": mount_options or [],
                "foreground": foreground,
                "consider_checkpoints": consider_checkpoints,
                "paths": paths,
                "strip_components": strip_components,
                "glob_archives": glob_archives,
                "first_n": first_n,
                "last_n": last_n,
            }
            
            # Create mount job record
            mount_job = MountJob(
                repository_id=repository_id,
                archive_name=archive_name,
                mount_point=mount_point_path,
                custom_mount_point=custom_mount_point,
                mount_options=json.dumps(mount_options_dict),
                status="pending",
                mounted_at=None
            )
            db.add(mount_job)
            db.commit()
            db.refresh(mount_job)

            logger.info("Created mount job",
                       job_id=mount_job.id,
                       repository_id=repository_id,
                       archive_name=archive_name,
                       mount_point=mount_point_path)

            # Execute mount in background
            asyncio.create_task(
                self._execute_mount(
                    mount_job.id,
                    repository_id,
                    archive_name,
                    mount_point_path,
                    mount_options or [],
                    foreground,
                    consider_checkpoints,
                    paths,
                    strip_components,
                    glob_archives,
                    first_n,
                    last_n,
                    db
                )
            )

            return mount_job

        except Exception as e:
            logger.error("Failed to create mount job", error=str(e))
            if db:
                db.rollback()
            raise

    async def _execute_mount(
        self,
        job_id: int,
        repository_id: int,
        archive_name: str,
        mount_point: str,
        mount_options: list,
        foreground: bool,
        consider_checkpoints: bool,
        paths: str,
        strip_components: str,
        glob_archives: str,
        first_n: str,
        last_n: str,
        db: Session
    ):
        """Execute mount operation"""
        try:
            # Get job and repository
            job = db.query(MountJob).filter(MountJob.id == job_id).first()
            if not job:
                logger.error("Mount job not found", job_id=job_id)
                return

            repository = db.query(Repository).filter(Repository.id == repository_id).first()
            if not repository:
                logger.error("Repository not found", repository_id=repository_id)
                job.status = "failed"
                job.error_message = f"Repository not found (ID: {repository_id})"
                job.unmounted_at = datetime.utcnow()
                db.commit()
                return

            # Update job status
            job.status = "mounting"
            job.mounted_at = datetime.utcnow()
            db.commit()

            # Call borg mount
            result = await borg.mount_repository(
                repository=repository.path,
                mount_point=mount_point,
                archive_name=archive_name,
                mount_options=mount_options,
                remote_path=repository.remote_path,
                passphrase=repository.passphrase,
                ssh_key_id=repository.ssh_key_id if repository.repository_type in ["ssh", "sftp"] else None,
                foreground=foreground,
                consider_checkpoints=consider_checkpoints,
                paths=paths,
                strip_components=strip_components,
                glob_archives=glob_archives,
                first_n=first_n,
                last_n=last_n
            )

            if result.get("success"):
                # Mount succeeded - borg mount runs as daemon, so we need to track it
                # Try to verify mount status
                try:
                    # Check if mount point is actually mounted
                    import subprocess
                    # Try mountpoint command first (more reliable)
                    try:
                        check_result = subprocess.run(
                            ["mountpoint", "-q", mount_point],
                            capture_output=True,
                            timeout=2
                        )
                        is_mounted = check_result.returncode == 0
                    except (FileNotFoundError, subprocess.TimeoutExpired):
                        # mountpoint command not available, try alternative check
                        # Check if mount point exists and is a directory
                        mount_path = Path(mount_point)
                        is_mounted = mount_path.exists() and mount_path.is_dir()
                        # Additional check: try to list directory (will fail if not mounted)
                        if is_mounted:
                            try:
                                list(mount_path.iterdir())
                            except:
                                is_mounted = False

                    if is_mounted:
                        job.status = "mounted"
                        logger.info("Mount successful",
                                   job_id=job_id,
                                   mount_point=mount_point)
                    else:
                        # Mount point exists but not mounted - might be starting up
                        # Wait a moment and check again
                        await asyncio.sleep(2)
                        try:
                            check_result = subprocess.run(
                                ["mountpoint", "-q", mount_point],
                                capture_output=True,
                                timeout=2
                            )
                            is_mounted = check_result.returncode == 0
                        except (FileNotFoundError, subprocess.TimeoutExpired):
                            mount_path = Path(mount_point)
                            is_mounted = mount_path.exists() and mount_path.is_dir()
                            if is_mounted:
                                try:
                                    list(mount_path.iterdir())
                                except:
                                    is_mounted = False

                        if is_mounted:
                            job.status = "mounted"
                        else:
                            job.status = "failed"
                            job.error_message = "Mount point created but not mounted"
                            job.unmounted_at = datetime.utcnow()
                except Exception as e:
                    logger.warning("Could not verify mount status", error=str(e))
                    # Assume success if borg command succeeded
                    job.status = "mounted"
            else:
                job.status = "failed"
                job.error_message = result.get("stderr", "Unknown error")
                job.unmounted_at = datetime.utcnow()
                logger.error("Mount failed",
                           job_id=job_id,
                           error=job.error_message)

            db.commit()

        except Exception as e:
            logger.error("Error executing mount", job_id=job_id, error=str(e))
            try:
                job = db.query(MountJob).filter(MountJob.id == job_id).first()
                if job:
                    job.status = "failed"
                    job.error_message = str(e)
                    job.unmounted_at = datetime.utcnow()
                    db.commit()
            except:
                pass

    async def unmount_repository(
        self,
        repository_id: int,
        archive_name: str = None,
        db: Session = None
    ) -> bool:
        """
        Unmount a repository or archive
        
        Args:
            repository_id: Repository ID
            archive_name: Optional archive name (if None, unmounts all mounts for repository)
            db: Database session
            
        Returns:
            True if successful
        """
        if db is None:
            db = SessionLocal()

        try:
            # Find mount job(s)
            query = db.query(MountJob).filter(
                MountJob.repository_id == repository_id,
                MountJob.status == "mounted"
            )
            
            if archive_name:
                query = query.filter(MountJob.archive_name == archive_name)
            
            mount_jobs = query.all()

            if not mount_jobs:
                raise ValueError("No active mounts found")

            success = True
            for job in mount_jobs:
                try:
                    # Call borg umount
                    result = await borg.unmount_repository(job.mount_point)

                    if result.get("success"):
                        job.status = "unmounted"
                        job.unmounted_at = datetime.utcnow()
                        logger.info("Unmount successful",
                                   job_id=job.id,
                                   mount_point=job.mount_point)

                        # Clean up mount point directory if empty
                        try:
                            mount_path = Path(job.mount_point)
                            if mount_path.exists():
                                # Try to remove if empty
                                try:
                                    mount_path.rmdir()
                                except OSError:
                                    # Directory not empty or other error - leave it
                                    pass
                        except Exception as e:
                            logger.warning("Failed to clean up mount point",
                                         mount_point=job.mount_point,
                                         error=str(e))
                    else:
                        job.status = "failed"
                        job.error_message = result.get("stderr", "Unknown error")
                        success = False
                        logger.error("Unmount failed",
                                   job_id=job.id,
                                   error=job.error_message)

                    db.commit()

                except Exception as e:
                    logger.error("Error unmounting", job_id=job.id, error=str(e))
                    job.status = "failed"
                    job.error_message = str(e)
                    success = False
                    db.commit()

            return success

        except Exception as e:
            logger.error("Failed to unmount", error=str(e))
            if db:
                db.rollback()
            raise

    def get_mount_status(self, repository_id: int, archive_name: str = None, db: Session = None) -> dict:
        """
        Get mount status for a repository or archive
        
        Args:
            repository_id: Repository ID
            archive_name: Optional archive name
            db: Database session
            
        Returns:
            Dict with mount status information
        """
        if db is None:
            db = SessionLocal()

        try:
            query = db.query(MountJob).filter(
                MountJob.repository_id == repository_id,
                MountJob.status == "mounted"
            )

            if archive_name:
                query = query.filter(MountJob.archive_name == archive_name)
                mount_job = query.first()
                if mount_job:
                    mount_opts = json.loads(mount_job.mount_options) if mount_job.mount_options else {}
                    return {
                        "mounted": True,
                        "mount_point": mount_job.mount_point,
                        "mounted_at": mount_job.mounted_at.isoformat() if mount_job.mounted_at else None,
                        "archive_name": mount_job.archive_name,
                        "mount_options": mount_opts.get("options", []) if isinstance(mount_opts, dict) else mount_opts,
                        "mount_config": mount_opts if isinstance(mount_opts, dict) else {}
                    }
                else:
                    return {"mounted": False}
            else:
                # Get all mounts for repository
                mount_jobs = query.all()
                if mount_jobs:
                    return {
                        "mounted": True,
                        "mounts": [
                            {
                                "mount_point": job.mount_point,
                                "mounted_at": job.mounted_at.isoformat() if job.mounted_at else None,
                                "archive_name": job.archive_name,
                                "mount_options": (lambda opts: opts.get("options", []) if isinstance(opts, dict) else opts)(json.loads(job.mount_options) if job.mount_options else {}),
                                "mount_config": json.loads(job.mount_options) if job.mount_options else {}
                            }
                            for job in mount_jobs
                        ]
                    }
                else:
                    return {"mounted": False}

        except Exception as e:
            logger.error("Failed to get mount status", error=str(e))
            return {"mounted": False, "error": str(e)}

    async def cleanup_stale_mounts(self, db: Session = None):
        """
        Clean up stale mounts from previous container runs
        
        This should be called on startup to handle cases where the container
        was restarted but mounts were left active.
        """
        if db is None:
            db = SessionLocal()

        try:
            # Find all mounts that are marked as mounted but the process is gone
            mount_jobs = db.query(MountJob).filter(
                MountJob.status == "mounted"
            ).all()

            cleaned = 0
            for job in mount_jobs:
                # Check if mount point is still mounted
                try:
                    import subprocess
                    is_mounted = False
                    try:
                        check_result = subprocess.run(
                            ["mountpoint", "-q", job.mount_point],
                            capture_output=True,
                            timeout=2
                        )
                        is_mounted = check_result.returncode == 0
                    except (FileNotFoundError, subprocess.TimeoutExpired):
                        # mountpoint command not available, try alternative check
                        mount_path = Path(job.mount_point)
                        is_mounted = mount_path.exists() and mount_path.is_dir()
                        if is_mounted:
                            try:
                                list(mount_path.iterdir())
                            except:
                                is_mounted = False

                    if not is_mounted:
                        # Not mounted anymore - mark as unmounted
                        job.status = "unmounted"
                        job.unmounted_at = datetime.utcnow()
                        cleaned += 1
                        logger.info("Cleaned up stale mount",
                                   job_id=job.id,
                                   mount_point=job.mount_point)
                except Exception as e:
                    logger.warning("Could not check mount status",
                                 job_id=job.id,
                                 error=str(e))

            if cleaned > 0:
                db.commit()
                logger.info("Cleaned up stale mounts", count=cleaned)

        except Exception as e:
            logger.error("Failed to cleanup stale mounts", error=str(e))
            if db:
                db.rollback()

# Global instance
mount_service = MountService()

