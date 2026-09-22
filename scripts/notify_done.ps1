# Windows toast notification for task-cycle completion.
# Usage:
#   powershell -NoProfile -File scripts/notify_done.ps1
#   powershell -NoProfile -File scripts/notify_done.ps1 -Message "F24-T1 done"
#
# The AppID MUST be the registered Windows PowerShell AUMID — a plain
# "PowerShell" string is an unregistered app ID and Windows 11 silently
# drops its toasts (no error, no popup).

param(
    [string]$Title = "Stock ingestion - tasks done",
    [string]$Message = "All registered tasks are complete."
)

[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
[Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] | Out-Null

$appId = '{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\WindowsPowerShell\v1.0\powershell.exe'

$safeTitle   = [System.Security.SecurityElement]::Escape($Title)
$safeMessage = [System.Security.SecurityElement]::Escape($Message)

$xml = New-Object Windows.Data.Xml.Dom.XmlDocument
$xml.LoadXml("<toast><visual><binding template=`"ToastGeneric`"><text>$safeTitle</text><text>$safeMessage</text></binding></visual></toast>")

$toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier($appId).Show($toast)
