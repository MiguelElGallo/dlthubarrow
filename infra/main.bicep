@description('Short environment name used in resource naming.')
param environmentName string

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Container image for the Azure Container App.')
param containerAppImage string = 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'

@description('CPU cores for the container app as a numeric string.')
param containerCpu string = '0.25'

@description('Container memory setting.')
param containerMemory string = '0.5Gi'

@secure()
@description('API key required to trigger benchmark runs.')
param benchmarkApiKey string = newGuid()

@description('Snowflake source account identifier.')
param sourceSnowflakeAccount string

@description('Snowflake source user.')
param sourceSnowflakeUser string

@secure()
@description('Snowflake source PAT.')
param sourceSnowflakeToken string

@description('Snowflake source warehouse.')
param sourceSnowflakeWarehouse string = 'COMPUTE_WH'

@description('Snowflake source role.')
param sourceSnowflakeRole string = ''

@description('Snowflake source database.')
param sourceSnowflakeDatabase string = 'SNOWFLAKE_SAMPLE_DATA'

@description('Snowflake destination account identifier.')
param destinationSnowflakeAccount string

@description('Snowflake destination user.')
param destinationSnowflakeUser string

@secure()
@description('Snowflake destination PAT.')
param destinationSnowflakeToken string

@description('Snowflake destination warehouse.')
param destinationSnowflakeWarehouse string = 'COMPUTE_WH'

@description('Snowflake destination role.')
param destinationSnowflakeRole string = ''

@description('Snowflake destination database.')
param destinationSnowflakeDatabase string = 'dummy'

var uniqueSuffix = toLower(uniqueString(subscription().id, resourceGroup().id, environmentName))
var containerAppsEnvironmentName = 'cae-${environmentName}'
var containerAppName = 'ca-${environmentName}'
var containerAppIdentityName = 'id-ca-${environmentName}'
var acrName = 'acr${uniqueSuffix}'
var keyVaultName = 'kv-${substring(uniqueSuffix, 0, 10)}'
var appInsightsName = 'appi-${environmentName}'
var logAnalyticsName = 'law-${environmentName}'
var secretUris = {
  benchmarkApiKey: '${keyvault.outputs.keyVaultUri}secrets/benchmark-api-key'
  sourceSnowflakeToken: '${keyvault.outputs.keyVaultUri}secrets/source-snowflake-token'
  destinationSnowflakeToken: '${keyvault.outputs.keyVaultUri}secrets/destination-snowflake-token'
}

resource containerAppIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: containerAppIdentityName
  location: location
}

module keyvault './modules/keyvault.bicep' = {
  name: 'keyvault'
  params: {
    location: location
    keyVaultName: keyVaultName
    containerAppPrincipalId: containerAppIdentity.properties.principalId
    secrets: [
      {
        name: 'benchmark-api-key'
        value: benchmarkApiKey
      }
      {
        name: 'source-snowflake-token'
        value: sourceSnowflakeToken
      }
      {
        name: 'destination-snowflake-token'
        value: destinationSnowflakeToken
      }
    ]
  }
}

module acr './modules/acr.bicep' = {
  name: 'acr'
  params: {
    location: location
    acrName: acrName
    pullPrincipalId: containerAppIdentity.properties.principalId
  }
}

module appinsights './modules/appinsights.bicep' = {
  name: 'appinsights'
  params: {
    location: location
    applicationInsightsName: appInsightsName
    logAnalyticsWorkspaceName: logAnalyticsName
  }
}

module containerApp './modules/container-app.bicep' = {
  name: 'containerApp'
  params: {
    location: location
    containerAppsEnvironmentName: containerAppsEnvironmentName
    containerAppName: containerAppName
    containerAppIdentityId: containerAppIdentity.id
    containerAppImage: containerAppImage
    containerCpu: containerCpu
    containerMemory: containerMemory
    acrLoginServer: acr.outputs.acrLoginServer
    appInsightsConnectionString: appinsights.outputs.applicationInsightsConnectionString
    logAnalyticsCustomerId: appinsights.outputs.logAnalyticsCustomerId
    logAnalyticsSharedKey: appinsights.outputs.logAnalyticsSharedKey
    sourceSnowflakeAccount: sourceSnowflakeAccount
    sourceSnowflakeUser: sourceSnowflakeUser
    sourceSnowflakeWarehouse: sourceSnowflakeWarehouse
    sourceSnowflakeRole: sourceSnowflakeRole
    sourceSnowflakeDatabase: sourceSnowflakeDatabase
    destinationSnowflakeAccount: destinationSnowflakeAccount
    destinationSnowflakeUser: destinationSnowflakeUser
    destinationSnowflakeWarehouse: destinationSnowflakeWarehouse
    destinationSnowflakeRole: destinationSnowflakeRole
    destinationSnowflakeDatabase: destinationSnowflakeDatabase
    secretUris: secretUris
  }
}

output CONTAINER_APP_NAME string = containerApp.outputs.containerAppName
output CONTAINER_APP_IDENTITY_NAME string = containerAppIdentityName
output CONTAINER_APP_IDENTITY_CLIENT_ID string = containerAppIdentity.properties.clientId
output CONTAINER_APP_IDENTITY_PRINCIPAL_ID string = containerAppIdentity.properties.principalId
output ACR_NAME string = acr.outputs.acrName
output ACR_LOGIN_SERVER string = acr.outputs.acrLoginServer
output AZURE_CONTAINER_REGISTRY_ENDPOINT string = acr.outputs.acrLoginServer
output AZURE_CONTAINER_REGISTRY_NAME string = acr.outputs.acrName
output KEY_VAULT_NAME string = keyvault.outputs.keyVaultName
output KEY_VAULT_URI string = keyvault.outputs.keyVaultUri
output APPLICATION_INSIGHTS_NAME string = appinsights.outputs.applicationInsightsName
output LOG_ANALYTICS_WORKSPACE_NAME string = appinsights.outputs.logAnalyticsWorkspaceName
