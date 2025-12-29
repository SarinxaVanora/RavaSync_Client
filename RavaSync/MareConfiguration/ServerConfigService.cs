using RavaSync.MareConfiguration.Configurations;

namespace RavaSync.MareConfiguration;

public class ServerConfigService : ConfigurationServiceBase<ServerConfig>
{
    public const string ConfigName = "server.json";

    public ServerConfigService(string configDir) : base(configDir)
    {
    }

    public override string ConfigurationName => ConfigName;
}