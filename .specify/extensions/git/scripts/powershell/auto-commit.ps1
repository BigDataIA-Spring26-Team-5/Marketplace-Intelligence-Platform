# Auto-commit changes for Spec Kit
# Usage: .\auto-commit.ps1 <event_name>

param(
    [Parameter(Mandatory=$true)]
    [string]$EventName
)

$ConfigFile = ".specify/extensions/git/git-config.yml"

# Check if git is available
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Warning "Git is not available. Skipping auto-commit."
    exit 0
}

# Check if we're in a git repository
try {
    $null = git rev-parse --git-dir 2>$null
} catch {
    Write-Warning "Not in a git repository. Skipping auto-commit."
    exit 0
}

# Check if config file exists
if (-not (Test-Path $ConfigFile)) {
    Write-Warning "Config file not found at $ConfigFile. Skipping auto-commit."
    exit 0
}

function Get-ConfigValue {
    param(
        [string]$Key,
        [string]$DefaultValue
    )
    
    $content = Get-Content $ConfigFile -Raw
    
    # Try to get event-specific config first
    $pattern = "(?s)$EventName:.*?enabled:\s*(true|false)"
    $match = [regex]::Match($content, $pattern)
    
    if ($match.Success) {
        return $match.Groups[1].Value
    }
    
    # Fall back to default
    $pattern = "default:\s*(true|false)"
    $match = [regex]::Match($content, $pattern)
    
    if ($match.Success) {
        return $match.Groups[1].Value
    }
    
    return $DefaultValue
}

function Get-CommitMessage {
    param([string]$Event)
    
    $content = Get-Content $ConfigFile -Raw
    
    # Try to get event-specific message
    $pattern = "(?s)$Event:.*?message:\s*(.+)"
    $match = [regex]::Match($content, $pattern)
    
    if ($match.Success) {
        $message = $match.Groups[1].Value.Trim()
        if ($message -ne "null") {
            return $message
        }
    }
    
    # Default message
    return "[Spec Kit] $Event"
}

# Check if auto-commit is enabled for this event
$Enabled = Get-ConfigValue -Key $EventName -DefaultValue "false"

if ($Enabled -ne "true") {
    Write-Host "Auto-commit is disabled for event: $EventName"
    exit 0
}

# Check if there are any changes to commit
$status = git status --porcelain
if ([string]::IsNullOrWhiteSpace($status)) {
    Write-Host "No changes to commit for event: $EventName"
    exit 0
}

# Get commit message
$CommitMessage = Get-CommitMessage -Event $EventName

Write-Host "Auto-committing changes for event: $EventName"
Write-Host "Commit message: $CommitMessage"

# Stage all changes
git add .

# Create commit
git commit -m $CommitMessage

Write-Host "Successfully committed changes for event: $EventName"