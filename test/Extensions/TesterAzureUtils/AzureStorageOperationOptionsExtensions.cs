using Azure.Core.Diagnostics;
using Azure.Data.Tables;
using Azure.Identity;
using TestExtensions;

namespace Tester.AzureUtils
{
    public static class AzureStorageOperationOptionsExtensions
    {
        static AzureStorageOperationOptionsExtensions()
        {
            AzureEventSourceListener.CreateConsoleLogger();
        }
        public static DefaultAzureCredentialOptions Options = new DefaultAzureCredentialOptions
        {
            AdditionallyAllowedTenants = { "*" },
            Diagnostics =
            {
                LoggedHeaderNames = { "x-ms-request-id" },
                LoggedQueryParameters = { "api-version" },
                IsAccountIdentifierLoggingEnabled = true,
            }
        };

        //public static DefaultAzureCredential Credential = new DefaultAzureCredential(Options);
        public static WorkloadIdentityCredential Credential = new WorkloadIdentityCredential();

        public static Orleans.Clustering.AzureStorage.AzureStorageOperationOptions ConfigureTestDefaults(this Orleans.Clustering.AzureStorage.AzureStorageOperationOptions options)
        {
            options.TableServiceClient = GetTableServiceClient();

            return options;
        }

        public static TableServiceClient GetTableServiceClient()
        {
            return TestDefaultConfiguration.UseAadAuthentication
                ? new(TestDefaultConfiguration.TableEndpoint, Credential)
                : new(TestDefaultConfiguration.DataConnectionString);
        }

        public static Orleans.GrainDirectory.AzureStorage.AzureStorageOperationOptions ConfigureTestDefaults(this Orleans.GrainDirectory.AzureStorage.AzureStorageOperationOptions options)
        {
            options.TableServiceClient = GetTableServiceClient();

            return options;
        }

        public static Orleans.Persistence.AzureStorage.AzureStorageOperationOptions ConfigureTestDefaults(this Orleans.Persistence.AzureStorage.AzureStorageOperationOptions options)
        {
            options.TableServiceClient = GetTableServiceClient();

            return options;
        }

        public static Orleans.Reminders.AzureStorage.AzureStorageOperationOptions ConfigureTestDefaults(this Orleans.Reminders.AzureStorage.AzureStorageOperationOptions options)
        {
            options.TableServiceClient = GetTableServiceClient();

            return options;
        }

        public static Orleans.Configuration.AzureBlobStorageOptions ConfigureTestDefaults(this Orleans.Configuration.AzureBlobStorageOptions options)
        {
            if (TestDefaultConfiguration.UseAadAuthentication)
            {
                options.BlobServiceClient = new(TestDefaultConfiguration.DataBlobUri, Credential);
            }
            else
            {
                options.BlobServiceClient = new(TestDefaultConfiguration.DataConnectionString);
            }

            return options;
        }

        public static Orleans.Configuration.AzureQueueOptions ConfigureTestDefaults(this Orleans.Configuration.AzureQueueOptions options)
        {
            if (TestDefaultConfiguration.UseAadAuthentication)
            {
                Console.WriteLine("BPETIT DEBUG: '" + Environment.GetEnvironmentVariable("AZURE_CLIENT_ID") + "'");
                Console.WriteLine("BPETIT DEBUG: '" + Environment.GetEnvironmentVariable("AZURE_TENANT_ID") + "'");
                Console.WriteLine("BPETIT DEBUG: '" + Environment.GetEnvironmentVariable("AZURE_FEDERATED_TOKEN_FILE") + "'");
                options.QueueServiceClient = new(TestDefaultConfiguration.DataQueueUri, Credential);
            }
            else
            {
                options.QueueServiceClient = new(TestDefaultConfiguration.DataConnectionString);
            }

            return options;
        }

        public static Orleans.Configuration.AzureBlobLeaseProviderOptions ConfigureTestDefaults(this Orleans.Configuration.AzureBlobLeaseProviderOptions options)
        {
            if (TestDefaultConfiguration.UseAadAuthentication)
            {
                options.BlobServiceClient = new(TestDefaultConfiguration.DataBlobUri, Credential);
            }
            else
            {
                options.BlobServiceClient = new(TestDefaultConfiguration.DataConnectionString);
            }

            return options;
        }
    }
}
