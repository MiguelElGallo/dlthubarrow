param location string
param containerAppsEnvironmentName string
param containerAppName string
param containerAppIdentityId string
param containerAppImage string
param containerCpu string
param containerMemory string
param acrLoginServer string
param appInsightsConnectionString string
param logAnalyticsCustomerId string
param sourceSnowflakeAccount string
param sourceSnowflakeUser string
param sourceSnowflakeWarehouse string
param sourceSnowflakeRole string
param sourceSnowflakeDatabase string
param destinationSnowflakeAccount string
param destinationSnowflakeUser string
param destinationSnowflakeWarehouse string
param destinationSnowflakeRole string
param destinationSnowflakeDatabase string

@secure()
param logAnalyticsSharedKey string

param secretUris object

resource containerAppsEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: containerAppsEnvironmentName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalyticsCustomerId
        sharedKey: logAnalyticsSharedKey
      }
    }
  }
}

resource containerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: containerAppName
  location: location
  tags: {
    'azd-service-name': 'benchmark-runner'
  }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${containerAppIdentityId}': {}
    }
  }
  properties: {
    managedEnvironmentId: containerAppsEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: [
        {
          name: 'benchmark-api-key'
          keyVaultUrl: string(secretUris.benchmarkApiKey)
          identity: containerAppIdentityId
        }
        {
          name: 'source-snowflake-token'
          keyVaultUrl: string(secretUris.sourceSnowflakeToken)
          identity: containerAppIdentityId
        }
        {
          name: 'destination-snowflake-token'
          keyVaultUrl: string(secretUris.destinationSnowflakeToken)
          identity: containerAppIdentityId
        }
      ]
      registries: [
        {
          server: acrLoginServer
          identity: containerAppIdentityId
        }
      ]
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
      }
    }
    template: {
      containers: [
        {
          name: 'app'
          image: containerAppImage
          resources: {
            cpu: json(containerCpu)
            memory: containerMemory
          }
          env: [
            {
              name: 'PORT'
              value: '8080'
            }
            {
              name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
              value: appInsightsConnectionString
            }
            {
              name: 'RUN_API_KEY'
              secretRef: 'benchmark-api-key'
            }
            {
              name: 'SOURCE_SNOWFLAKE_ACCOUNT'
              value: sourceSnowflakeAccount
            }
            {
              name: 'SOURCE_SNOWFLAKE_USER'
              value: sourceSnowflakeUser
            }
            {
              name: 'SOURCE_SNOWFLAKE_PASSWORD'
              secretRef: 'source-snowflake-token'
            }
            {
              name: 'SOURCE_SNOWFLAKE_WAREHOUSE'
              value: sourceSnowflakeWarehouse
            }
            {
              name: 'SOURCE_SNOWFLAKE_ROLE'
              value: sourceSnowflakeRole
            }
            {
              name: 'SOURCE_SNOWFLAKE_DATABASE'
              value: sourceSnowflakeDatabase
            }
            {
              name: 'DESTINATION_SNOWFLAKE_ACCOUNT'
              value: destinationSnowflakeAccount
            }
            {
              name: 'DESTINATION_SNOWFLAKE_USER'
              value: destinationSnowflakeUser
            }
            {
              name: 'DESTINATION_SNOWFLAKE_PASSWORD'
              secretRef: 'destination-snowflake-token'
            }
            {
              name: 'DESTINATION_SNOWFLAKE_WAREHOUSE'
              value: destinationSnowflakeWarehouse
            }
            {
              name: 'DESTINATION_SNOWFLAKE_ROLE'
              value: destinationSnowflakeRole
            }
            {
              name: 'DESTINATION_SNOWFLAKE_DATABASE'
              value: destinationSnowflakeDatabase
            }
            {
              name: 'BENCHMARK_DATASETS'
              value: 'TPCH_SF1,TPCH_SF10,TPCH_SF100,TPCH_SF1000'
            }
            {
              name: 'BENCHMARK_SOURCE_TABLE'
              value: 'LINEITEM'
            }
            {
              name: 'BENCHMARK_SOURCE_CHUNK_ROWS'
              value: '50000'
            }
            {
              name: 'BENCHMARK_WORK_ROOT'
              value: '/tmp/dlthubarrow'
            }
          ]
          probes: [
            {
              type: 'liveness'
              httpGet: {
                path: '/healthz'
                port: 8080
              }
              periodSeconds: 10
              initialDelaySeconds: 15
              failureThreshold: 3
            }
            {
              type: 'readiness'
              httpGet: {
                path: '/healthz'
                port: 8080
              }
              periodSeconds: 5
              initialDelaySeconds: 5
              failureThreshold: 3
            }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

output containerAppName string = containerApp.name
