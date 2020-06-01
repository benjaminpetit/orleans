using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Runtime;
using StackExchange.Redis;

namespace Orleans.GrainDirectory.Redis
{
    public class RedisGrainDirectory : IGrainDirectory, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly RedisGrainDirectoryOptions directoryOptions;
        private readonly ClusterOptions clusterOptions;
        private readonly ILogger<RedisGrainDirectory> logger;

        private ConnectionMultiplexer redis;
        private IDatabase database;

        public RedisGrainDirectory(
            RedisGrainDirectoryOptions directoryOptions,
            IOptions<ClusterOptions> clusterOptions,
            ILogger<RedisGrainDirectory> logger)
        {
            this.directoryOptions = directoryOptions;
            this.logger = logger;
            this.clusterOptions = clusterOptions.Value;
        }

        public async Task<GrainAddress> Lookup(string grainId)
        {
            try
            {
                var result = (string)await this.database.StringGetAsync(GetKey(grainId));

                if (this.logger.IsEnabled(LogLevel.Debug))
                    this.logger.LogDebug("Lookup {GrainId}: {Result}", grainId, string.IsNullOrWhiteSpace(result) ? "null" : result);

                if (string.IsNullOrWhiteSpace(result))
                    return default;

                return JsonConvert.DeserializeObject<GrainAddress>(result);
            }
            catch (RedisException ex)
            {
                this.logger.LogError(ex, "Lookup failed for {GrainId}", grainId);
                throw new OrleansException($"Lookup failed for {grainId} : {ex.ToString()}");
            }
        }

        public async Task<GrainAddress> Register(GrainAddress address)
        {
            var value = JsonConvert.SerializeObject(address);

            try
            {
                var success = await this.database.StringSetAsync(
                    this.GetKey(address.GrainId),
                    value,
                    this.directoryOptions.EntryExpiry,
                    When.NotExists);

                if (this.logger.IsEnabled(LogLevel.Debug))
                    this.logger.LogDebug("Register {GrainId} ({Address}): {Result}", address.GrainId, value, success ? "OK" : "Conflict");

                if (success)
                    return address;

                return await Lookup(address.GrainId);
            }
            catch (RedisException ex)
            {
                this.logger.LogError(ex, "Register failed for {GrainId} ({Address})", address.GrainId, value);
                throw new OrleansException($"Register failed for {address.GrainId} ({value}) : {ex.ToString()}");
            }
        }

        public async Task Unregister(GrainAddress address)
        {
            var key = GetKey(address.GrainId);
            var value = JsonConvert.SerializeObject(address);

            try
            {
                var tx = this.database.CreateTransaction();
                tx.AddCondition(Condition.StringEqual(key, value));
                tx.KeyDeleteAsync(key).Ignore();
                var success = await tx.ExecuteAsync();

                if (this.logger.IsEnabled(LogLevel.Debug))
                    this.logger.LogDebug("Unregister {GrainId} ({Address}): {Result}", address.GrainId, value, success ? "OK" : "Conflict");
            }
            catch (RedisException ex)
            {
                this.logger.LogError(ex, "Unregister failed for {GrainId} ({Address})", address.GrainId, value);
                throw new OrleansException($"Unregister failed for {address.GrainId} ({value}) : {ex.ToString()}");
            }
        }

        public Task UnregisterSilos(List<string> siloAddresses)
        {
            return Task.CompletedTask;
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(nameof(RedisGrainDirectory), ServiceLifecycleStage.RuntimeInitialize, Initialize, Uninitialize);
        }

        public async Task Initialize(CancellationToken ct = default)
        {
            this.redis = await ConnectionMultiplexer.ConnectAsync(this.directoryOptions.ConfigurationOptions);

            // Configure logging
            this.redis.ConnectionRestored += this.LogConnectionRestored;
            this.redis.ConnectionFailed += this.LogConnectionFailed;
            this.redis.ErrorMessage += this.LogErrorMessage;
            this.redis.InternalError += this.LogInternalError;
            this.redis.IncludeDetailInExceptions = true;

            this.database = this.redis.GetDatabase();
        }

        private async Task Uninitialize(CancellationToken arg)
        {
            if (this.redis != null && this.redis.IsConnected)
            {
                await this.redis.CloseAsync();
                this.redis.Dispose();
                this.redis = null;
                this.database = null;
            }
        }

        private string GetKey(string grainId) => $"{this.clusterOptions.ClusterId}-{grainId}";

        #region Logging
        private void LogConnectionRestored(object sender, ConnectionFailedEventArgs e)
            => this.logger.LogInformation(e.Exception, "Connection to {EndPoint) failed: {FailureType}", e.EndPoint, e.FailureType);

        private void LogConnectionFailed(object sender, ConnectionFailedEventArgs e)
            => this.logger.LogError(e.Exception, "Connection to {EndPoint) failed: {FailureType}", e.EndPoint, e.FailureType);

        private void LogErrorMessage(object sender, RedisErrorEventArgs e)
            => this.logger.LogError(e.Message);

        private void LogInternalError(object sender, InternalErrorEventArgs e)
            => this.logger.LogError(e.Exception, "Internal error");
        #endregion
    }
}
