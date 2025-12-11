"""
Migration 028: Add Mount Jobs System

This migration creates the mount_jobs table to track Borg mount operations.
Enables mounting repositories and archives as FUSE filesystems.
"""

def upgrade(db):
    """Create mount_jobs table"""
    print("Running migration 028: Add Mount Jobs System")

    try:
        # Check if table already exists
        cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mount_jobs'")
        existing_table = cursor.fetchone()

        if not existing_table:
            print("Creating mount_jobs table...")
            db.execute("""
                CREATE TABLE mount_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repository_id INTEGER NOT NULL,
                    archive_name VARCHAR,
                    mount_point VARCHAR NOT NULL,
                    custom_mount_point BOOLEAN NOT NULL DEFAULT 0,
                    mount_options TEXT,
                    status VARCHAR NOT NULL DEFAULT 'mounted',
                    mounted_at TIMESTAMP,
                    unmounted_at TIMESTAMP,
                    process_pid INTEGER,
                    process_start_time BIGINT,
                    error_message TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (repository_id) REFERENCES repositories(id)
                )
            """)
            db.execute("CREATE INDEX ix_mount_jobs_repository_id ON mount_jobs(repository_id)")
            db.execute("CREATE INDEX ix_mount_jobs_status ON mount_jobs(status)")
            print("✓ Created mount_jobs table")
        else:
            print("✓ Table mount_jobs already exists, skipping")

        db.commit()
        print("✓ Migration 028 completed successfully")

    except Exception as e:
        db.rollback()
        print(f"✗ Migration 028 failed: {e}")
        raise

def downgrade(db):
    """Remove mount_jobs table"""
    print("Running migration 028 downgrade: Remove Mount Jobs System")

    try:
        cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mount_jobs'")
        existing_table = cursor.fetchone()

        if existing_table:
            print("Dropping mount_jobs table...")
            db.execute("DROP TABLE mount_jobs")
            print("✓ Dropped mount_jobs table")
        else:
            print("✓ Table mount_jobs does not exist, skipping")

        db.commit()
        print("✓ Migration 028 downgrade completed successfully")

    except Exception as e:
        db.rollback()
        print(f"✗ Migration 028 downgrade failed: {e}")
        raise

