#!/usr/bin/env python3
import os
import json
import tempfile
import zipfile
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
import subprocess

class YAMLGeneratorHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.serve_html()
        else:
            self.send_error(404)
    
    def do_POST(self):
        if self.path == '/generate-yaml':
            self.generate_yaml()
        else:
            self.send_error(404)
    
    def serve_html(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        html_path = os.path.join(script_dir, 'agent-generator.html')
        
        with open(html_path, 'r') as f:
            content = f.read()
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(content.encode())
    
    def generate_yaml(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode('utf-8'))
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        
        # Set environment variables
        env = os.environ.copy()
        env.update({
            'AGENT_NAME': data['agentName'],
            'CONFIG_MAP_NAME=pg-kafka-snowflake-streamer-{data['agentName']}",
            'SECRET_NAME=pg-kafka-snowflake-streamer-{data['agentName']}",
            'PVC_CLAIM': 'redis-data-pvc',
            'NAMESPACE': f"cdc-{data['environment']}",
            'ENVIRONMENT': data['environment'],
            'DEBEZIUM_COMMON_PROPS': 'debezium-common-props',
            'POSTGRES_HOST': data['postgresHost'],
            'POSTGRES_DATABASE': data['postgresDatabase'],
            'POSTGRES_USER': data['postgresUser'],
            'POSTGRES_PASSWORD': data['postgresPassword'],
            'POSTGRES_PORT': '5432',
            'SNOWFLAKE_SCHEMA': data['snowflakeSchema'],
            'SNOWFLAKE_PASSWORD': data['snowflakePassword'],
            'REDIS_PASSWORD': data['redisPassword'],
            'SCHEMA_INCLUDE_LIST': data['schemaIncludeList'],
            'REPLICATION_SLOT_NAME': data['replicationSlotName'],
            'PUBLICATION_NAME': data['publicationName'],
            'TABLE_EXCLUDE_LIST': data.get('tableExcludeList', ''),
            'KAFKA_BOOTSTRAP_SERVERS': data['kafkaBootstrapServers'],
            'KAFKA_CONSUMER_CLIENT_ID': data['kafkaConsumerClientId'],
            'KAFKA_CONSUMER_GROUP_ID': data['kafkaConsumerGroupId'],
            'KAFKA_CONSUMER_TOPIC_PATTERN': data['kafkaConsumerTopicPattern'],
            'KAFKA_TOPIC_PREFIX': data['kafkaTopicPrefix'],
            'AWS_ACCESS_KEY_ID': data['awsAccessKeyId'],
            'AWS_SECRET_ACCESS_KEY': data['awsSecretAccessKey']
        })
        
        # Set ECR registry based on environment
        if data['environment'] == 'prod':
            ecr_registry = os.environ.get('ECR_REGISTRY_PROD', 'YOUR_ACCOUNT_ID.dkr.ecr.YOUR_REGION.amazonaws.com')
        else:
            ecr_registry = os.environ.get('ECR_REGISTRY_NONPROD', 'YOUR_ACCOUNT_ID.dkr.ecr.YOUR_REGION.amazonaws.com')
        
        env['DOCKER_IMAGE'] = f"{ecr_registry}/cdc-{data['environment']}/pg-kafka-snowflake-streamer:latest"
        
        # Generate YAML files
        with tempfile.TemporaryDirectory() as temp_dir:
            templates = ['configmap-template.yaml', 'secret-template.yaml', 'deployment-template.yaml']
            generated_files = []
            
            for template in templates:
                template_path = os.path.join(project_root, 'k8s', template)
                output_name = template.replace('-template', f"-{data['agentName']}")
                output_path = os.path.join(temp_dir, output_name)
                
                # Use envsubst to substitute variables
                with open(template_path, 'r') as f:
                    template_content = f.read()
                
                result = subprocess.run(['envsubst'], input=template_content, 
                                      text=True, capture_output=True, env=env)
                
                with open(output_path, 'w') as f:
                    f.write(result.stdout)
                
                generated_files.append(output_path)
            
            # Create ZIP file
            zip_path = os.path.join(temp_dir, f"{data['agentName']}-k8s-manifests.zip")
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for file_path in generated_files:
                    zipf.write(file_path, os.path.basename(file_path))
            
            # Send ZIP file
            with open(zip_path, 'rb') as f:
                zip_content = f.read()
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/zip')
            self.send_header('Content-Disposition', f'attachment; filename="{data["agentName"]}-k8s-manifests.zip"')
            self.send_header('Content-Length', str(len(zip_content)))
            self.end_headers()
            self.wfile.write(zip_content)

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8000), YAMLGeneratorHandler)
    print("Server running at http://localhost:8000")
    server.serve_forever()