"""
Script to handle DynamoDB operations for Twitter network data
"""
import json
from pathlib import Path
from typing import List, Dict, Union, Optional

import boto3
from botocore.exceptions import ClientError


class DatabaseHandler:
    # Default AWS configuration
    DYNAMODB_TABLE_NAME = "UserEngagement"

    def __init__(self):
        """
        Initialize the DynamoDB handler.
        """
        # Initialize DynamoDB client
        self.dynamodb = boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(self.DYNAMODB_TABLE_NAME)

    def load_from_json(self, json_path: Union[str, Path]) -> List[Dict]:
        """
        Load data from a JSON file.
        
        Args:
            json_path (Union[str, Path]): Path to the JSON file
            
        Returns:
            List[Dict]: List of dictionary objects from the JSON file
        """
        with open(json_path, "r") as f:
            data = json.load(f)
        return data.get("edges", []) if isinstance(data, dict) else data

    def store_data(self, data: List[Dict]) -> Dict[str, int]:
        """
        Store data in DynamoDB table.
        
        Args:
            data (List[Dict]): List of dictionary objects to store
            
        Returns:
            Dict[str, int]: Statistics about the operation
                - "successful": Number of successful writes
                - "failed": Number of failed writes
        """
        stats = {"successful": 0, "failed": 0}
        
        for item in data:
            try:
                # Ensure required fields are present
                if not all(key in item for key in ["source", "target"]):
                    print(f"Skipping item missing required fields: {item}")
                    stats["failed"] += 1
                    continue
                    
                # Add item to DynamoDB
                self.table.put_item(Item=item)
                stats["successful"] += 1
                
            except ClientError as e:
                print(f"Error storing item {item}: {str(e)}")
                stats["failed"] += 1
                
        return stats

    def process_json_file(self, json_path: Union[str, Path]) -> Dict[str, int]:
        """
        Load data from JSON file and store it in DynamoDB.
        
        Args:
            json_path (Union[str, Path]): Path to the JSON file
            
        Returns:
            Dict[str, int]: Statistics about the operation
        """
        data = self.load_from_json(json_path)
        return self.store_data(data)

    def batch_store_data(
        self,
        data: List[Dict],
        batch_size: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Store data in DynamoDB using batch write operations.
        
        Args:
            data (List[Dict]): List of dictionary objects to store
            batch_size (int, optional): Number of items to write in each batch.
                If None, uses the instance's batch_size.
            
        Returns:
            Dict[str, int]: Statistics about the operation
        """
        stats = {"successful": 0, "failed": 0}
        
        # Process data in batches
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            try:
                with self.table.batch_writer() as batch_writer:
                    for item in batch:
                        if not all(key in item for key in ["source", "target"]):
                            print(f"Skipping item missing required fields: {item}")
                            stats["failed"] += 1
                            continue
                            
                        batch_writer.put_item(Item=item)
                        stats["successful"] += 1
                        
            except ClientError as e:
                print(f"Error in batch write: {str(e)}")
                stats["failed"] += len(batch)
                
        return stats 