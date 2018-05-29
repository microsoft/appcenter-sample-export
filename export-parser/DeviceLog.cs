namespace AppCenter.ExportParser
{
    public class DeviceLog
    {
        public string AppId { get; set; }
        public string Timestamp { get; set; }
        public string InstallId { get; set; }
        public string MessageType { get; set; }
        public string IngressTimestamp { get; set; }
        public string MessageId { get; set; }
        public string EventId { get; set; }
        public string EventName { get; set; }
        public string CorrelationId { get; set; }
        public string IsTestMessage { get; set; }
        public string SdkVersion { get; set; }
        public string Model { get; set; }
        public string OemName { get; set; }
        public string OsName { get; set; }
        public string OsVersion { get; set; }
        public string OsApiLevel { get; set; }
        public string Locale { get; set; }
        public string TimeZoneOffset { get; set; }
        public string ScreenSize { get; set; }
        public string AppVersion { get; set; }
        public string AppBuild { get; set; }
        public string AppNamespace { get; set; }
        public string CarrierName { get; set; }
        public string CarrierCountry { get; set; }
        public string CountryCode { get; set; }
        public string WrapperSdkVersion { get; set; }
        public string WrapperSdkName { get; set; }
        public string Properties { get; set; }
        public string SessionId { get; set; }
        public string SdkName { get; set; }
        public string OsBuild { get; set; }
        public string WrapperRuntimeVersion { get; set; }
        public string LiveUpdateDeploymentKey { get; set; }
        public string LiveUpdatePackageHash { get; set; }
        public string LiveUpdateReleaseLabel { get; set; }
    }
}
