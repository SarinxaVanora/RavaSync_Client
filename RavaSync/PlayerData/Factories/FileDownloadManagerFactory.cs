using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI.Files;
using Microsoft.Extensions.Logging;

namespace RavaSync.PlayerData.Factories;

public class FileDownloadManagerFactory
{
    private readonly FileCacheManager _fileCacheManager;
    private readonly FileCompactor _fileCompactor;
    private readonly FileTransferOrchestrator _fileTransferOrchestrator;
    private readonly ILoggerFactory _loggerFactory;
    private readonly MareMediator _mareMediator;
    private readonly MareConfigService _mareConfigService;
    private readonly DelayedActivatorService _delayedActivator;

    public FileDownloadManagerFactory(
        ILoggerFactory loggerFactory,
        MareMediator mareMediator,
        FileTransferOrchestrator fileTransferOrchestrator,
        FileCacheManager fileCacheManager,
        FileCompactor fileCompactor,
        MareConfigService mareConfigService,
        DelayedActivatorService delayedActivator)
    {
        _loggerFactory = loggerFactory;
        _mareMediator = mareMediator;
        _fileTransferOrchestrator = fileTransferOrchestrator;
        _fileCacheManager = fileCacheManager;
        _fileCompactor = fileCompactor;
        _mareConfigService = mareConfigService;
        _delayedActivator = delayedActivator;
    }

    public FileDownloadManager Create()
    {
        return new FileDownloadManager(
            _loggerFactory.CreateLogger<FileDownloadManager>(),
            _mareMediator,
            _fileTransferOrchestrator,
            _fileCacheManager,
            _fileCompactor,
            _mareConfigService,
            _delayedActivator);
    }
}
