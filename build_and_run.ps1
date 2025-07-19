# build_and_run.ps1
# Portable script to build Rust modules and run ANQuant application

# Configuration
$PROJECT_DIR = "D:\AlphaNivesh\ANQuant"
$VENV_PATH = "$PROJECT_DIR\.venv\Scripts\Activate.ps1"
$VCPKG_DIR = "E:\Tools\vcpkg"
$PYTHON_EXE = "$PROJECT_DIR\.venv\Scripts\python.exe"
$VS_BUILD_TOOLS_PATH = "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat"
$CMAKE_GENERATOR = "Visual Studio 17 2022"
$SITE_PACKAGES = "$PROJECT_DIR\.venv\Lib\site-packages"

function Install-Dependency {
    param ($command, $wingetId, $installCommand)
    Write-Host "Checking $command..."
    & $command --version 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Installing $command..."
        & $installCommand
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Error: Failed to install $command" -ForegroundColor Red
            exit 1
        }
    }
}

Write-Host "Checking project directory..."
if (-not (Test-Path $PROJECT_DIR)) {
    Write-Host "Error: Project directory $PROJECT_DIR not found" -ForegroundColor Red
    exit 1
}

Install-Dependency -command "git" -wingetId "Git.Git" -installCommand "winget install --id Git.Git -e --source winget"
Install-Dependency -command "cmake" -wingetId "Kitware.CMake" -installCommand "winget install --id Kitware.CMake -e --source winget"
Install-Dependency -command "rustc" -wingetId "" -installCommand "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"

Write-Host "Checking virtual environment..."
if (-not (Test-Path $VENV_PATH)) {
    Write-Host "Creating virtual environment..."
    python -m venv "$PROJECT_DIR\.venv"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Failed to create virtual environment" -ForegroundColor Red
        exit 1
    }
}
Write-Host "Activating virtual environment..."
& $VENV_PATH
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to activate virtual environment" -ForegroundColor Red
    exit 1
}

Write-Host "Updating pip..."
& $PYTHON_EXE -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to update pip" -ForegroundColor Red
    exit 1
}

Write-Host "Installing Python dependencies..."
pip uninstall PyCrypto -y
$smartApiDir = "C:\Tools\smartapi-python"
if (Test-Path $smartApiDir) {
    Remove-Item -Path $smartApiDir -Recurse -Force
}
git clone https://github.com/angel-one/smartapi-python.git $smartApiDir
cd $smartApiDir
$setupPy = Get-Content setup.py
$setupPy = $setupPy -replace "'?PyCrypto'?", "'pycryptodome'"
$setupPy | Set-Content setup.py
pip install .
cd $PROJECT_DIR
pip install pyyaml hvac loguru redis confluent-kafka pyotp pandas websocket-client maturin
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to install Python dependencies" -ForegroundColor Red
    exit 1
}

Write-Host "Setting up vcpkg..."
if (Test-Path $VCPKG_DIR) {
    Remove-Item -Path $VCPKG_DIR -Recurse -Force
}
git clone https://github.com/microsoft/vcpkg.git $VCPKG_DIR
cd $VCPKG_DIR
.\bootstrap-vcpkg.bat
if (-not (Test-Path "vcpkg.exe")) {
    Write-Host "Error: vcpkg.exe not created" -ForegroundColor Red
    exit 1
}
.\vcpkg install zstd:x64-windows
.\vcpkg integrate install
$env:LIBZSTD_LIB_DIR = "$VCPKG_DIR\installed\x64-windows\lib"
$env:LIBZSTD_INCLUDE_DIR = "$VCPKG_DIR\installed\x64-windows\include"
$env:CMAKE_GENERATOR = $CMAKE_GENERATOR
[System.Environment]::SetEnvironmentVariable("LIBZSTD_LIB_DIR", $env:LIBZSTD_LIB_DIR, "User")
[System.Environment]::SetEnvironmentVariable("LIBZSTD_INCLUDE_DIR", $env:LIBZSTD_INCLUDE_DIR, "User")

Write-Host "Setting Visual Studio Build Tools environment..."
if (Test-Path $VS_BUILD_TOOLS_PATH) {
    & $VS_BUILD_TOOLS_PATH
} else {
    Write-Host "Error: Visual Studio Build Tools not found at $VS_BUILD_TOOLS_PATH" -ForegroundColor Red
    exit 1
}

Write-Host "Building indicator_engine..."
cd "$PROJECT_DIR\src\rs\indicator"
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "Error: Cargo.toml not found" -ForegroundColor Red
    exit 1
}
maturin develop --release
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to build indicator_engine" -ForegroundColor Red
    exit 1
}

Write-Host "Building market_data_engine..."
cd "$PROJECT_DIR\src\rs\market_data"
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "Error: Cargo.toml not found" -ForegroundColor Red
    exit 1
}
maturin develop --release
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to build market_data_engine" -ForegroundColor Red
    exit 1
}

Write-Host "Clearing Python cache..."
Remove-Item -Path "$PROJECT_DIR\__pycache__" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "$PROJECT_DIR\src\py\__pycache__" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "$PROJECT_DIR\src\py\util\__pycache__" -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Ensuring __init__.py files..."
New-Item -ItemType File -Path "$PROJECT_DIR\src\__init__.py" -Force | Out-Null
New-Item -ItemType File -Path "$PROJECT_DIR\src\py\__init__.py" -Force | Out-Null
New-Item -ItemType File -Path "$PROJECT_DIR\src\py\util\__init__.py" -Force | Out-Null

Write-Host "Ensuring offline mode in config.yaml..."
$configPath = "$PROJECT_DIR\config\config.yaml"
if (Test-Path $configPath) {
    $configContent = Get-Content $configPath
    if ($configContent -notmatch "offline_mode: true") {
        $configContent = $configContent -replace "offline_mode:.*", "offline_mode: true"
        $configContent | Set-Content $configPath
    }
} else {
    Set-Content -Path $configPath -Value "global:`n  offline_mode: true"
}

Write-Host "Running main.py..."
cd "$PROJECT_DIR\src\py"
$env:PYTHONPATH = "$PROJECT_DIR\src\py"
& $PYTHON_EXE main.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to run main.py" -ForegroundColor Red
    exit 1
}

Write-Host "ANQuant application executed successfully!" -ForegroundColor Green