# ============================================================
# Script: create_resources.ps1
# Purpose: Deploy all Azure resources for Data Warehouse project
# Author: Data Warehouse Team
# Date: 2024
# ============================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$Location,
    
    [Parameter(Mandatory=$false)]
    [string]$Environment = "prod",
    
    [Parameter(Mandatory=$false)]
    [string]$StorageAccountName = "",
    
    [Parameter(Mandatory=$false)]
    [string]$DataFactoryName = "",
    
    [Parameter(Mandatory=$false)]
    [string]$SynapseWorkspaceName = "",
    
    [Parameter(Mandatory=$false)]
    [string]$SqlPoolName = "sqldwedw",
    
    [Parameter(Mandatory=$false)]
    [string]$SqlAdminUsername = "sqladminuser",
    
    [Parameter(Mandatory=$false)]
    [securestring]$SqlAdminPassword = (Read-Host -Prompt "Enter SQL Admin Password" -AsSecureString),
    
    [Parameter(Mandatory=$false)]
    [string]$ShirVmName = "shir-vm",
    
    [Parameter(Mandatory=$false)]
    [string]$ShirVmSize = "Standard_D2s_v3",
    
    [Parameter(Mandatory=$false)]
    [string]$ShirVmAdminUsername = "azureuser",
    
    [Parameter(Mandatory=$false)]
    [securestring]$ShirVmAdminPassword = (Read-Host -Prompt "Enter SHIR VM Admin Password" -AsSecureString),
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipSynapse = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipDataFactory = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipShirVm = $false
)

# ============================================================
# FUNCTIONS
# ============================================================

function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

function Test-ResourceExists {
    param([string]$ResourceName, [string]$ResourceType)
    $exists = az resource show --name $ResourceName --resource-group $ResourceGroupName --resource-type $ResourceType --query "id" -o tsv 2>$null
    return ($null -ne $exists -and $exists -ne "")
}

# ============================================================
# VALIDATION
# ============================================================

Write-ColorOutput "========================================" "Cyan"
Write-ColorOutput "DATA WAREHOUSE RESOURCE DEPLOYMENT" "Cyan"
Write-ColorOutput "========================================" "Cyan"
Write-ColorOutput "Environment: $Environment" "Yellow"
Write-ColorOutput "Location: $Location" "Yellow"
Write-ColorOutput "Resource Group: $ResourceGroupName" "Yellow"
Write-ColorOutput ""

# Generate unique names if not provided
if ([string]::IsNullOrEmpty($StorageAccountName)) {
    $StorageAccountName = "adlsedw$Environment$(Get-Random -Minimum 1000 -Maximum 9999)"
}
if ([string]::IsNullOrEmpty($DataFactoryName)) {
    $DataFactoryName = "adf-edw-$Environment"
}
if ([string]::IsNullOrEmpty($SynapseWorkspaceName)) {
    $SynapseWorkspaceName = "synapse-edw-$Environment"
}

# ============================================================
# STEP 1: LOGIN TO AZURE
# ============================================================

Write-ColorOutput "STEP 1: Logging into Azure..." "Green"
az login --only-show-errors
if ($LASTEXITCODE -ne 0) {
    Write-ColorOutput "ERROR: Failed to login to Azure" "Red"
    exit 1
}
Write-ColorOutput "✓ Logged into Azure successfully" "Green"
Write-ColorOutput ""

# ============================================================
# STEP 2: CREATE RESOURCE GROUP
# ============================================================

Write-ColorOutput "STEP 2: Creating Resource Group..." "Green"
$rgExists = az group exists --name $ResourceGroupName --query "exists" -o tsv
if ($rgExists -eq "true") {
    Write-ColorOutput "✓ Resource Group '$ResourceGroupName' already exists" "Yellow"
} else {
    az group create --name $ResourceGroupName --location $Location --only-show-errors
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput "ERROR: Failed to create resource group" "Red"
        exit 1
    }
    Write-ColorOutput "✓ Resource Group '$ResourceGroupName' created" "Green"
}
Write-ColorOutput ""

# ============================================================
# STEP 3: CREATE STORAGE ACCOUNT (ADLS GEN2)
# ============================================================

Write-ColorOutput "STEP 3: Creating Storage Account (ADLS Gen2)..." "Green"

if (Test-ResourceExists $StorageAccountName "Microsoft.Storage/storageAccounts") {
    Write-ColorOutput "✓ Storage Account '$StorageAccountName' already exists" "Yellow"
} else {
    az storage account create `
        --name $StorageAccountName `
        --resource-group $ResourceGroupName `
        --location $Location `
        --sku Standard_LRS `
        --kind StorageV2 `
        --hierarchical-namespace true `
        --only-show-errors
    
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput "ERROR: Failed to create storage account" "Red"
        exit 1
    }
    Write-ColorOutput "✓ Storage Account '$StorageAccountName' created" "Green"
}

# Get storage account key
$storageKey = az storage account keys list `
    --account-name $StorageAccountName `
    --resource-group $ResourceGroupName `
    --query "[0].value" -o tsv

# Create containers
Write-ColorOutput "Creating containers..." "Green"
$containers = @("bronze", "silver", "gold", "staging")

foreach ($container in $containers) {
    az storage container create `
        --name $container `
        --account-name $StorageAccountName `
        --account-key $storageKey `
        --only-show-errors 2>$null
    
    if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne 1) {
        Write-ColorOutput "  ⚠ Container '$container' may already exist" "Yellow"
    } else {
        Write-ColorOutput "  ✓ Container '$container' created" "Green"
    }
}
Write-ColorOutput ""

# ============================================================
# STEP 4: CREATE DATA FACTORY
# ============================================================

if (-not $SkipDataFactory) {
    Write-ColorOutput "STEP 4: Creating Data Factory..." "Green"
    
    if (Test-ResourceExists $DataFactoryName "Microsoft.DataFactory/factories") {
        Write-ColorOutput "✓ Data Factory '$DataFactoryName' already exists" "Yellow"
    } else {
        az datafactory create `
            --resource-group $ResourceGroupName `
            --factory-name $DataFactoryName `
            --location $Location `
            --only-show-errors
        
        if ($LASTEXITCODE -ne 0) {
            Write-ColorOutput "ERROR: Failed to create Data Factory" "Red"
            exit 1
        }
        Write-ColorOutput "✓ Data Factory '$DataFactoryName' created" "Green"
    }
    
    # Get Data Factory Managed Identity
    $dataFactoryIdentity = az datafactory show `
        --resource-group $ResourceGroupName `
        --factory-name $DataFactoryName `
        --query "identity.principalId" -o tsv
    
    Write-ColorOutput "✓ Data Factory Managed Identity: $dataFactoryIdentity" "Green"
    Write-ColorOutput ""
} else {
    Write-ColorOutput "STEP 4: Skipping Data Factory creation" "Yellow"
    Write-ColorOutput ""
}

# ============================================================
# STEP 5: CREATE SYNAPSE ANALYTICS
# ============================================================

if (-not $SkipSynapse) {
    Write-ColorOutput "STEP 5: Creating Synapse Analytics Workspace..." "Green"
    
    if (Test-ResourceExists $SynapseWorkspaceName "Microsoft.Synapse/workspaces") {
        Write-ColorOutput "✓ Synapse Workspace '$SynapseWorkspaceName' already exists" "Yellow"
    } else {
        # Convert securestring to plain text for Azure CLI
        $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SqlAdminPassword)
        $PlainPassword = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
        
        az synapse workspace create `
            --name $SynapseWorkspaceName `
            --resource-group $ResourceGroupName `
            --storage-account $StorageAccountName `
            --file-system "bronze" `
            --sql-admin-login-user $SqlAdminUsername `
            --sql-admin-login-password $PlainPassword `
            --location $Location `
            --only-show-errors
        
        if ($LASTEXITCODE -ne 0) {
            Write-ColorOutput "ERROR: Failed to create Synapse Workspace" "Red"
            exit 1
        }
        Write-ColorOutput "✓ Synapse Workspace '$SynapseWorkspaceName' created" "Green"
        
        # Clear password from memory
        $PlainPassword = $null
    }
    
    # Create Dedicated SQL Pool
    Write-ColorOutput "Creating Dedicated SQL Pool '$SqlPoolName'..." "Green"
    
    az synapse sql pool create `
        --name $SqlPoolName `
        --workspace-name $SynapseWorkspaceName `
        --resource-group $ResourceGroupName `
        --performance-level "DWU100" `
        --only-show-errors
    
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput "  ⚠ SQL Pool may already exist or creation in progress" "Yellow"
    } else {
        Write-ColorOutput "  ✓ Dedicated SQL Pool '$SqlPoolName' created" "Green"
    }
    
    Write-ColorOutput ""
} else {
    Write-ColorOutput "STEP 5: Skipping Synapse Analytics creation" "Yellow"
    Write-ColorOutput ""
}

# ============================================================
# STEP 6: CREATE SHIR VIRTUAL MACHINE
# ============================================================

if (-not $SkipShirVm) {
    Write-ColorOutput "STEP 6: Creating Self-hosted Integration Runtime VM..." "Green"
    
    $shirVmFullName = "$ShirVmName-$Environment"
    
    if (Test-ResourceExists $shirVmFullName "Microsoft.Compute/virtualMachines") {
        Write-ColorOutput "✓ SHIR VM '$shirVmFullName' already exists" "Yellow"
    } else {
        # Convert securestring to plain text
        $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($ShirVmAdminPassword)
        $PlainPassword = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
        
        # Create VM
        az vm create `
            --resource-group $ResourceGroupName `
            --name $shirVmFullName `
            --location $Location `
            --image "Win2019Datacenter" `
            --size $ShirVmSize `
            --admin-username $ShirVmAdminUsername `
            --admin-password $PlainPassword `
            --public-ip-sku Standard `
            --only-show-errors
        
        if ($LASTEXITCODE -ne 0) {
            Write-ColorOutput "ERROR: Failed to create SHIR VM" "Red"
            exit 1
        }
        Write-ColorOutput "✓ SHIR VM '$shirVmFullName' created" "Green"
        
        # Open port 443 for outbound
        az vm open-port `
            --resource-group $ResourceGroupName `
            --name $shirVmFullName `
            --port 443 `
            --priority 1000 `
            --only-show-errors
        
        Write-ColorOutput "  ✓ Port 443 opened for outbound traffic" "Green"
        
        $PlainPassword = $null
    }
    Write-ColorOutput ""
} else {
    Write-ColorOutput "STEP 6: Skipping SHIR VM creation" "Yellow"
    Write-ColorOutput ""
}

# ============================================================
# STEP 7: DEPLOY ARM TEMPLATE (Optional)
# ============================================================

Write-ColorOutput "STEP 7: Deploying ARM Template..." "Green"

$armTemplatePath = Join-Path $PSScriptRoot "deploy_arm_template.json"

if (Test-Path $armTemplatePath) {
    az deployment group create `
        --resource-group $ResourceGroupName `
        --template-file $armTemplatePath `
        --parameters environment=$Environment location=$Location `
        --only-show-errors
    
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput "⚠ ARM template deployment had issues, continuing..." "Yellow"
    } else {
        Write-ColorOutput "✓ ARM template deployed successfully" "Green"
    }
} else {
    Write-ColorOutput "⚠ ARM template not found at: $armTemplatePath" "Yellow"
}
Write-ColorOutput ""

# ============================================================
# OUTPUT SUMMARY
# ============================================================

Write-ColorOutput "========================================" "Cyan"
Write-ColorOutput "DEPLOYMENT COMPLETE" "Cyan"
Write-ColorOutput "========================================" "Cyan"
Write-ColorOutput ""
Write-ColorOutput "RESOURCE SUMMARY:" "White"
Write-ColorOutput "  Resource Group: $ResourceGroupName" "Green"
Write-ColorOutput "  Storage Account: $StorageAccountName" "Green"
Write-ColorOutput "  Data Factory: $DataFactoryName" "Green"
Write-ColorOutput "  Synapse Workspace: $SynapseWorkspaceName" "Green"
Write-ColorOutput "  SQL Pool: $SqlPoolName" "Green"
Write-ColorOutput "  SHIR VM: $shirVmFullName" "Green"
Write-ColorOutput ""

Write-ColorOutput "NEXT STEPS:" "Yellow"
Write-ColorOutput "  1. Run setup/01_enable_cdc.sql on your source SQL Server" "White"
Write-ColorOutput "  2. Run setup/02_create_control_tables.sql on Synapse" "White"
Write-ColorOutput "  3. Run setup/03_create_silver_tables.sql on Synapse" "White"
Write-ColorOutput "  4. Run setup/04_create_gold_tables.sql on Synapse" "White"
Write-ColorOutput "  5. Install SHIR on the VM and register with Data Factory" "White"
Write-ColorOutput "  6. Import ADF pipelines from /adf-pipelines folder" "White"
Write-ColorOutput "  7. Upload notebooks to Synapse workspace" "White"
Write-ColorOutput "  8. Deploy stored procedures from /stored-procedures folder" "White"
Write-ColorOutput "  9. Configure Power BI dashboard" "White"
Write-ColorOutput ""

Write-ColorOutput "========================================" "Cyan"