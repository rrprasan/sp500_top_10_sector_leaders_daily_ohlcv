#!/usr/bin/env python3
"""
S3 Bucket Cleanup Script
Deletes all files from the sp500-top-10-sector-leaders-ohlcv-s3bkt bucket.
"""

import boto3
import logging
import sys
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('s3_cleanup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class S3BucketCleaner:
    """
    Class to handle S3 bucket cleanup operations.
    """
    
    def __init__(self):
        """Initialize the S3 bucket cleaner."""
        self.S3_BUCKET = 'sp500-top-10-sector-leaders-ohlcv-s3bkt'
        self.s3_client = None
        logger.info("S3 Bucket Cleaner initialized")
    
    def _initialize_s3_client(self) -> boto3.client:
        """Initialize AWS S3 client."""
        try:
            s3_client = boto3.client('s3')
            # Test the connection
            s3_client.head_bucket(Bucket=self.S3_BUCKET)
            logger.info(f"S3 client initialized successfully for bucket: {self.S3_BUCKET}")
            return s3_client
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except ClientError as e:
            logger.error(f"Failed to access S3 bucket {self.S3_BUCKET}: {e}")
            raise
    
    def list_all_objects(self) -> list:
        """List all objects in the S3 bucket."""
        try:
            if not self.s3_client:
                self.s3_client = self._initialize_s3_client()
            
            objects = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.S3_BUCKET):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append(obj['Key'])
            
            logger.info(f"Found {len(objects)} objects in bucket {self.S3_BUCKET}")
            return objects
            
        except Exception as e:
            logger.error(f"Failed to list objects in bucket: {e}")
            raise
    
    def delete_all_objects(self) -> bool:
        """Delete all objects from the S3 bucket."""
        try:
            objects = self.list_all_objects()
            
            if not objects:
                logger.info("No objects found in bucket. Nothing to delete.")
                return True
            
            logger.info(f"Starting deletion of {len(objects)} objects...")
            
            # Delete objects in batches of 1000 (AWS limit)
            batch_size = 1000
            deleted_count = 0
            
            for i in range(0, len(objects), batch_size):
                batch = objects[i:i + batch_size]
                
                # Prepare delete request
                delete_request = {
                    'Objects': [{'Key': obj} for obj in batch]
                }
                
                # Delete batch
                response = self.s3_client.delete_objects(
                    Bucket=self.S3_BUCKET,
                    Delete=delete_request
                )
                
                # Count successful deletions
                if 'Deleted' in response:
                    deleted_count += len(response['Deleted'])
                    logger.info(f"Deleted batch of {len(response['Deleted'])} objects")
                
                # Log any errors
                if 'Errors' in response:
                    for error in response['Errors']:
                        logger.error(f"Failed to delete {error['Key']}: {error['Message']}")
            
            logger.info(f"Successfully deleted {deleted_count} objects from bucket {self.S3_BUCKET}")
            return deleted_count == len(objects)
            
        except Exception as e:
            logger.error(f"Failed to delete objects from bucket: {e}")
            return False
    
    def verify_bucket_empty(self) -> bool:
        """Verify that the bucket is empty after cleanup."""
        try:
            objects = self.list_all_objects()
            is_empty = len(objects) == 0
            
            if is_empty:
                logger.info("✅ Bucket is now empty")
            else:
                logger.warning(f"⚠️  Bucket still contains {len(objects)} objects")
            
            return is_empty
            
        except Exception as e:
            logger.error(f"Failed to verify bucket status: {e}")
            return False
    
    def run_cleanup(self) -> bool:
        """Run the complete cleanup process."""
        logger.info("=" * 60)
        logger.info("S3 BUCKET CLEANUP PROCESS")
        logger.info("=" * 60)
        logger.info(f"Target bucket: {self.S3_BUCKET}")
        
        try:
            # Step 1: Initialize S3 client
            logger.info("Step 1: Initializing S3 client...")
            self.s3_client = self._initialize_s3_client()
            
            # Step 2: List current objects
            logger.info("Step 2: Listing current objects...")
            objects = self.list_all_objects()
            
            if not objects:
                logger.info("Bucket is already empty. No cleanup needed.")
                return True
            
            # Step 3: Confirm deletion
            logger.info(f"Step 3: About to delete {len(objects)} objects...")
            logger.info("Sample objects to be deleted:")
            for i, obj in enumerate(objects[:10]):  # Show first 10
                logger.info(f"  {i+1}. {obj}")
            if len(objects) > 10:
                logger.info(f"  ... and {len(objects) - 10} more objects")
            
            # Step 4: Delete all objects
            logger.info("Step 4: Deleting all objects...")
            success = self.delete_all_objects()
            
            if not success:
                logger.error("Cleanup failed during deletion process")
                return False
            
            # Step 5: Verify cleanup
            logger.info("Step 5: Verifying cleanup...")
            is_empty = self.verify_bucket_empty()
            
            if is_empty:
                logger.info("🎉 S3 bucket cleanup completed successfully!")
                logger.info("The bucket is now ready for new data uploads.")
            else:
                logger.error("❌ Cleanup verification failed")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Cleanup process failed: {e}")
            return False


def main():
    """Main entry point."""
    try:
        cleaner = S3BucketCleaner()
        success = cleaner.run_cleanup()
        
        if success:
            logger.info("S3 bucket cleanup completed successfully")
            sys.exit(0)
        else:
            logger.error("S3 bucket cleanup failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Cleanup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
