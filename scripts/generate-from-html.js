const fs = require('fs');
const path = require('path');

const projectRoot = path.dirname(__dirname);
const outputDir = path.join(projectRoot, 'k8s', 'generated');

// Clear existing files
if (fs.existsSync(outputDir)) {
    fs.rmSync(outputDir, { recursive: true });
}
fs.mkdirSync(outputDir, { recursive: true });

// Read form data from stdin
let input = '';
process.stdin.on('data', chunk => input += chunk);
process.stdin.on('end', () => {
    const data = JSON.parse(input);
    
    const ecrRegistry = data.environment === 'prod' 
        ? process.env.ECR_REGISTRY_PROD || 'YOUR_ACCOUNT_ID.dkr.ecr.YOUR_REGION.amazonaws.com'
        : process.env.ECR_REGISTRY_NONPROD || 'YOUR_ACCOUNT_ID.dkr.ecr.YOUR_REGION.amazonaws.com';
    
    const vars = {
        AGENT_NAME: data.agentName,
        CONFIG_MAP_NAME: `pg-kafka-snowflake-streamer-${data.agentName}`,
        SECRET_NAME: `pg-kafka-snowflake-streamer-${data.agentName}`,
        NAMESPACE: `cdc-${data.environment}`,
        ENVIRONMENT: data.environment,
        DEBEZIUM_COMMON_PROPS: 'debezium-common-props',
        PVC_CLAIM: 'redis-data-pvc',
        DOCKER_IMAGE: `${ecrRegistry}/cdc-${data.environment}/pg-kafka-snowflake-streamer:latest`,
        POSTGRES_HOST: data.postgresHost,
        POSTGRES_DATABASE: data.postgresDatabase,
        POSTGRES_USER: data.postgresUser,
        POSTGRES_PASSWORD: data.postgresPassword,
        POSTGRES_PORT: '5432',
        SNOWFLAKE_SCHEMA: data.snowflakeSchema,
        SNOWFLAKE_PASSWORD: data.snowflakePassword,
        REDIS_PASSWORD: data.redisPassword,
        SCHEMA_INCLUDE_LIST: data.schemaIncludeList,
        REPLICATION_SLOT_NAME: data.replicationSlotName,
        PUBLICATION_NAME: data.publicationName,
        TABLE_EXCLUDE_LIST: data.tableExcludeList || '',
        KAFKA_BOOTSTRAP_SERVERS: data.kafkaBootstrapServers,
        KAFKA_CONSUMER_CLIENT_ID: data.kafkaConsumerClientId,
        KAFKA_CONSUMER_GROUP_ID: data.kafkaConsumerGroupId,
        KAFKA_CONSUMER_TOPIC_PATTERN: data.kafkaConsumerTopicPattern,
        KAFKA_TOPIC_PREFIX: data.kafkaTopicPrefix,
        AWS_ACCESS_KEY_ID: data.awsAccessKeyId,
        AWS_SECRET_ACCESS_KEY: data.awsSecretAccessKey
    };
    
    function substitute(template, vars) {
        return template.replace(/\$\{([^}]+)\}/g, (match, key) => {
            const cleanKey = key.replace(':-}', '').replace(':-', '');
            return vars[cleanKey] || '';
        });
    }
    
    // Read templates and generate files
    const templates = ['configmap-template.yaml', 'secret-template.yaml', 'deployment-template.yaml'];
    
    templates.forEach(template => {
        const templatePath = path.join(projectRoot, 'k8s', template);
        const templateContent = fs.readFileSync(templatePath, 'utf8');
        const processedContent = substitute(templateContent, vars);
        const outputFile = template.replace('-template', `-${data.agentName}`);
        const outputPath = path.join(outputDir, outputFile);
        
        fs.writeFileSync(outputPath, processedContent);
        console.log(`Generated: ${outputFile}`);
    });
    
    console.log(`\nFiles generated in: ${outputDir}`);
    console.log(`To deploy: kubectl apply -f ${outputDir}/${data.agentName}-*.yaml`);
});