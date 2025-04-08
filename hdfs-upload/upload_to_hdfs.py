#!/usr/bin/env python3

"""
TPC-DS HDFS Upload Script.
This script uploads the formatted TPC-DS data to both HDFS clusters (Simple-auth and Kerberized).
Uses Python native libraries instead of shell commands.
"""

import os
import argparse
import logging
import sys
import time
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

sys.path.append(str(Path(__file__).parent.parent))
from utils.command import run_command, PythonCommand

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hdfs_upload.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# TPC-DS Tables
TPC_DS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site"
]

# Data formats
FORMATS = ["csv", "json", "pipe", "orc", "parquet"]


class HDFSPythonClient:
    """
    Python-native HDFS client that doesn't rely on shell commands.
    Uses pyarrow.hdfs or pydoop.hdfs depending on availability.
    """
    
    def __init__(self, hadoop_conf_dir: str = None, 
                 use_kerberos: bool = None, 
                 principal: str = None):
        """
        Initialize the HDFS client
        
        Args:
            hadoop_conf_dir: Path to Hadoop configuration directory
            use_kerberos: Whether to use Kerberos authentication
            principal: Kerberos principal
        """
        self.hadoop_conf_dir = hadoop_conf_dir
        self.hdfs = None
        self.use_kerberos = use_kerberos
        self.principal = principal
        self._initialize_client()
        
    def _initialize_client(self) -> None:
        """Initialize the appropriate HDFS client based on available libraries"""
        # Try to determine if Kerberos should be used if not explicitly specified
        if self.use_kerberos is None:
            self.use_kerberos = self._should_use_kerberos()
            
        logger.debug(f"HDFS client initialization - Using Kerberos: {self.use_kerberos}")
            
        # Try to use pyarrow first, fall back to pydoop, then hdfs
        try:
            import pyarrow.hdfs
            logger.info("Using pyarrow.hdfs for HDFS operations")
            
            # Parse Hadoop configuration to get connection details
            host, port = self._get_hdfs_address_from_conf()
            
            # Additional options for Kerberos
            extra_conf = self._get_hdfs_conf()
            
            # Create connection options
            conn_kwargs = {
                'host': host,
                'port': port,
                'user': None,  # Will use current user or HADOOP_USER_NAME env var
                'extra_conf': extra_conf
            }
            
            # Add Kerberos options if needed
            if self.use_kerberos:
                conn_kwargs['kerb_ticket'] = True  # Use the ticket cache
                
                # Set principal if available
                if self.principal:
                    conn_kwargs['user'] = self.principal.split('@')[0]  # Extract username part
            
            self.hdfs = pyarrow.hdfs.connect(**conn_kwargs)
            self.client_type = 'pyarrow'
        except ImportError:
            try:
                import pydoop.hdfs as hdfs
                logger.info("Using pydoop.hdfs for HDFS operations")
                self.hdfs = hdfs
                self.client_type = 'pydoop'
            except ImportError:
                try:
                    import hdfs
                    logger.info("Using hdfs for HDFS operations")
                    # Parse Hadoop configuration to get connection details
                    host, port = self._get_hdfs_address_from_conf()
                    
                    # Create connection options
                    conn_kwargs = {
                        'url': f'http://{host}:{port}',
                        'root': '/',
                    }
                    
                    # Add Kerberos authentication if needed
                    if self.use_kerberos:
                        try:
                            import requests_kerberos
                            conn_kwargs['session'] = requests.Session()
                            conn_kwargs['session'].auth = requests_kerberos.HTTPKerberosAuth(
                                mutual_authentication=requests_kerberos.OPTIONAL)
                            logger.info("Using Kerberos authentication for hdfs client")
                        except ImportError:
                            logger.warning("requests_kerberos not available, Kerberos auth may not work")
                    
                    self.hdfs = hdfs.InsecureClient(**conn_kwargs)
                    self.client_type = 'hdfs'
                except ImportError:
                    logger.error("No HDFS client library found. Please install pyarrow, pydoop, or hdfs.")
                    raise ImportError("No HDFS client library available")
    
    def _get_hdfs_address_from_conf(self) -> Tuple[str, int]:
        """
        Parse Hadoop configuration to get HDFS namenode address
        
        Returns:
            Tuple of (host, port)
        """
        # Default values
        host = 'localhost'
        port = 8020  # Default HDFS port
        
        if self.hadoop_conf_dir:
            try:
                # Try to parse core-site.xml
                import xml.etree.ElementTree as ET
                core_site_path = os.path.join(self.hadoop_conf_dir, 'core-site.xml')
                
                if os.path.exists(core_site_path):
                    tree = ET.parse(core_site_path)
                    root = tree.getroot()
                    
                    # Find fs.defaultFS property
                    for prop in root.findall('./property'):
                        name = prop.find('name')
                        value = prop.find('value')
                        
                        if name is not None and value is not None and name.text == 'fs.defaultFS':
                            # Parse hdfs://host:port
                            fs_url = value.text
                            if fs_url.startswith('hdfs://'):
                                address = fs_url[7:]  # Remove 'hdfs://'
                                if ':' in address:
                                    host, port_str = address.split(':', 1)
                                    try:
                                        port = int(port_str)
                                    except ValueError:
                                        pass  # Use default port
            except Exception as e:
                logger.warning(f"Error parsing Hadoop configuration: {e}")
                
        return host, port
    
    def _get_hdfs_conf(self) -> Dict[str, str]:
        """
        Get Hadoop configuration as dictionary
        
        Returns:
            Dictionary of configuration properties
        """
        conf = {}
        
        if self.hadoop_conf_dir:
            try:
                import xml.etree.ElementTree as ET
                
                # Parse core-site.xml
                core_site_path = os.path.join(self.hadoop_conf_dir, 'core-site.xml')
                if os.path.exists(core_site_path):
                    tree = ET.parse(core_site_path)
                    root = tree.getroot()
                    
                    for prop in root.findall('./property'):
                        name = prop.find('name')
                        value = prop.find('value')
                        
                        if name is not None and value is not None:
                            conf[name.text] = value.text
                
                # Parse hdfs-site.xml
                hdfs_site_path = os.path.join(self.hadoop_conf_dir, 'hdfs-site.xml')
                if os.path.exists(hdfs_site_path):
                    tree = ET.parse(hdfs_site_path)
                    root = tree.getroot()
                    
                    for prop in root.findall('./property'):
                        name = prop.find('name')
                        value = prop.find('value')
                        
                        if name is not None and value is not None:
                            conf[name.text] = value.text
            except Exception as e:
                logger.warning(f"Error parsing Hadoop configuration: {e}")
                
        return conf
    
    def mkdir(self, path: str) -> bool:
        """
        Create directory in HDFS
        
        Args:
            path: HDFS path to create
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.client_type == 'pyarrow':
                self.hdfs.mkdir(path, create_parents=True)
            elif self.client_type == 'pydoop':
                self.hdfs.mkdir(path)
            elif self.client_type == 'hdfs':
                self.hdfs.makedirs(path)
            return True
        except Exception as e:
            logger.error(f"Error creating directory {path}: {e}")
            return False
    
    def upload_file(self, local_path: str, hdfs_path: str) -> bool:
        """
        Upload file to HDFS
        
        Args:
            local_path: Local file path
            hdfs_path: HDFS target path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.client_type == 'pyarrow':
                with open(local_path, 'rb') as local_file:
                    with self.hdfs.open(hdfs_path, 'wb') as hdfs_file:
                        hdfs_file.write(local_file.read())
            elif self.client_type == 'pydoop':
                self.hdfs.put(local_path, hdfs_path)
            elif self.client_type == 'hdfs':
                self.hdfs.upload(hdfs_path, local_path, overwrite=True)
            return True
        except Exception as e:
            logger.error(f"Error uploading file {local_path} to {hdfs_path}: {e}")
            return False
            
    def upload_directory(self, local_dir: str, hdfs_dir: str) -> bool:
        """
        Upload directory to HDFS
        
        Args:
            local_dir: Local directory path
            hdfs_dir: HDFS target directory
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create target directory
            self.mkdir(hdfs_dir)
            
            # Walk through local directory and upload files
            for root, dirs, files in os.walk(local_dir):
                # Get relative path
                rel_path = os.path.relpath(root, local_dir)
                if rel_path == '.':
                    rel_path = ''
                
                # Create directories
                for dir_name in dirs:
                    hdfs_subdir = os.path.join(hdfs_dir, rel_path, dir_name)
                    self.mkdir(hdfs_subdir)
                
                # Upload files
                for file_name in files:
                    local_file = os.path.join(root, file_name)
                    hdfs_file = os.path.join(hdfs_dir, rel_path, file_name)
                    if not self.upload_file(local_file, hdfs_file):
                        return False
            
            return True
        except Exception as e:
            logger.error(f"Error uploading directory {local_dir} to {hdfs_dir}: {e}")
            return False

    def _should_use_kerberos(self) -> bool:
        """
        Determine if Kerberos authentication should be used based on Hadoop configuration
        
        Returns:
            bool: True if Kerberos should be used
        """
        if not self.hadoop_conf_dir:
            return False
            
        try:
            # Check for Kerberos-related properties in Hadoop configuration
            hdfs_conf = self._get_hdfs_conf()
            auth_method = hdfs_conf.get('hadoop.security.authentication', '')
            
            if auth_method.lower() == 'kerberos':
                logger.info("Detected Kerberos authentication from Hadoop configuration")
                return True
                
            # Additional check in core-site.xml
            import xml.etree.ElementTree as ET
            core_site_path = os.path.join(self.hadoop_conf_dir, 'core-site.xml')
            
            if os.path.exists(core_site_path):
                tree = ET.parse(core_site_path)
                root = tree.getroot()
                
                for prop in root.findall('./property'):
                    name = prop.find('name')
                    value = prop.find('value')
                    
                    if name is not None and name.text == 'hadoop.security.authentication' and \
                       value is not None and value.text.lower() == 'kerberos':
                        logger.info("Detected Kerberos authentication from core-site.xml")
                        return True
        except Exception as e:
            logger.warning(f"Error checking for Kerberos configuration: {e}")
            
        return False


def upload_to_simple_auth_hdfs(
    data_dir: str,
    hdfs_target_dir: str,
    scale: int,
    format_type: str,
    hadoop_conf_dir: str,
    user: str
) -> bool:
    """
    Upload data to Simple-auth HDFS cluster using Python native HDFS client.
    
    Args:
        data_dir: Path to local data directory
        hdfs_target_dir: HDFS target directory
        scale: Scale factor
        format_type: Data format
        hadoop_conf_dir: Path to Hadoop configuration
        user: Hadoop user name
    
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Uploading {scale}GB {format_type} data to Simple-auth HDFS...")
    
    # Set environment variables
    old_user = os.environ.get('HADOOP_USER_NAME')
    os.environ['HADOOP_USER_NAME'] = user
    old_conf_dir = os.environ.get('HADOOP_CONF_DIR')
    os.environ['HADOOP_CONF_DIR'] = hadoop_conf_dir
    
    try:
        # Initialize HDFS client
        hdfs_client = HDFSPythonClient(hadoop_conf_dir)
        
        # Create target directory
        target_dir = f"{hdfs_target_dir}/{scale}gb/{format_type}"
        if not hdfs_client.mkdir(target_dir):
            return False
        
        # Upload each table
        for table in TPC_DS_TABLES:
            logger.info(f"Uploading {table}...")
            local_path = f"{data_dir}/{scale}gb/{format_type}/{table}"
            
            if not os.path.exists(local_path):
                logger.warning(f"Local path does not exist: {local_path}")
                continue
            
            target_path = f"{target_dir}/{table}"
            success = hdfs_client.upload_directory(local_path, target_path)
            
            if not success:
                logger.error(f"Failed to upload {table}")
                return False
                
            logger.info(f"Successfully uploaded {table} to Simple-auth HDFS")
        
        return True
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    
    finally:
        # Restore environment variables
        if old_user is not None:
            os.environ['HADOOP_USER_NAME'] = old_user
        else:
            os.environ.pop('HADOOP_USER_NAME', None)
            
        if old_conf_dir is not None:
            os.environ['HADOOP_CONF_DIR'] = old_conf_dir
        else:
            os.environ.pop('HADOOP_CONF_DIR', None)


def upload_to_kerberized_hdfs(
    data_dir: str,
    hdfs_target_dir: str,
    scale: int,
    format_type: str,
    hadoop_conf_dir: str,
    keytab: str,
    principal: str
) -> bool:
    """
    Upload data to Kerberized HDFS cluster using Python native HDFS client.
    
    Args:
        data_dir: Path to local data directory
        hdfs_target_dir: HDFS target directory
        scale: Scale factor
        format_type: Data format
        hadoop_conf_dir: Path to Hadoop configuration
        keytab: Path to the keytab file
        principal: Kerberos principal
    
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Uploading {scale}GB {format_type} data to Kerberized HDFS...")
    
    # Set environment variables
    old_conf_dir = os.environ.get('HADOOP_CONF_DIR')
    os.environ['HADOOP_CONF_DIR'] = hadoop_conf_dir
    
    try:
        # Check and obtain Kerberos ticket if needed
        have_ticket = False
        
        # Try multiple methods to check for Kerberos ticket
        methods_tried = []
        
        # Method 1: Use klist command
        try:
            klist_cmd = ["klist", "-s"]
            success, _, _ = run_command(klist_cmd, "Check Kerberos ticket")
            have_ticket = success
            methods_tried.append("klist command")
        except Exception as e:
            logger.debug(f"Could not check ticket with klist: {e}")
        
        # Method 2: Use gssapi library if available
        if not have_ticket:
            try:
                import gssapi
                # Get default credential store
                cred_store = gssapi.Credentials(usage='initiate')
                # If we can get credentials and they're not expired, we have a ticket
                have_ticket = cred_store.lifetime > 0
                methods_tried.append("gssapi")
            except ImportError:
                logger.debug("gssapi not available")
            except Exception as e:
                logger.debug(f"Could not check ticket with gssapi: {e}")
        
        # Method 3: Use kerberos library if available
        if not have_ticket:
            try:
                import kerberos
                # This will throw an exception if no valid ticket
                kerberos.checkPassword(principal, "")  # Empty password to just check if ticket exists
                have_ticket = True
                methods_tried.append("kerberos")
            except ImportError:
                logger.debug("kerberos module not available")
            except kerberos.KrbError:
                logger.debug("No valid Kerberos ticket found")
            except Exception as e:
                logger.debug(f"Could not check ticket with kerberos: {e}")
        
        logger.debug(f"Kerberos ticket check methods tried: {', '.join(methods_tried)}")
        
        # If no ticket, obtain one
        if not have_ticket:
            logger.info("No valid Kerberos ticket found. Obtaining a new one...")
            
            # Try multiple methods to obtain a ticket
            ticket_obtained = False
            
            # Method 1: Use gssapi library if available
            try:
                import gssapi
                import os.path
                
                if os.path.isfile(keytab):
                    name = gssapi.Name(principal, name_type=gssapi.NameType.kerberos_principal)
                    store = {'client_keytab': keytab, 'client_principal': str(name)}
                    cred = gssapi.Credentials(name=name, usage='initiate', store=store)
                    logger.info("Successfully obtained Kerberos ticket using gssapi")
                    ticket_obtained = True
            except ImportError:
                logger.debug("gssapi not available for obtaining ticket")
            except Exception as e:
                logger.warning(f"Could not obtain ticket with gssapi: {e}")
            
            # Method 2: Use subprocess with kinit as fallback
            if not ticket_obtained:
                try:
                    # Use subprocess directly for this specific operation
                    import subprocess
                    kinit_cmd = ["kinit", "-kt", keytab, principal]
                    result = subprocess.run(kinit_cmd, check=True, stderr=subprocess.PIPE)
                    logger.info("Successfully obtained Kerberos ticket using kinit")
                    ticket_obtained = True
                except Exception as e:
                    stderr_output = getattr(e, 'stderr', b'').decode('utf-8', errors='ignore')
                    logger.error(f"Failed to obtain Kerberos ticket: {e}. Error output: {stderr_output}")
                    return False
        
        # Initialize HDFS client with Kerberos support
        hdfs_client = HDFSPythonClient(hadoop_conf_dir, use_kerberos=have_ticket, principal=principal)
        
        # Create target directory
        target_dir = f"{hdfs_target_dir}/{scale}gb/{format_type}"
        if not hdfs_client.mkdir(target_dir):
            return False
        
        # Upload each table
        for table in TPC_DS_TABLES:
            logger.info(f"Uploading {table}...")
            local_path = f"{data_dir}/{scale}gb/{format_type}/{table}"
            
            if not os.path.exists(local_path):
                logger.warning(f"Local path does not exist: {local_path}")
                continue
            
            target_path = f"{target_dir}/{table}"
            success = hdfs_client.upload_directory(local_path, target_path)
            
            if not success:
                logger.error(f"Failed to upload {table}")
                return False
                
            logger.info(f"Successfully uploaded {table} to Kerberized HDFS")
        
        return True
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    
    finally:
        # Restore environment variables
        if old_conf_dir is not None:
            os.environ['HADOOP_CONF_DIR'] = old_conf_dir
        else:
            os.environ.pop('HADOOP_CONF_DIR', None)


def main():
    """Main function to upload TPC-DS data to HDFS clusters."""
    parser = argparse.ArgumentParser(description="Upload TPC-DS data to HDFS")
    parser.add_argument("--data-dir", default="../data/formatted", help="Path to formatted data")
    parser.add_argument("--hdfs-target-dir", default="/benchmark/tpcds", help="HDFS target directory")
    parser.add_argument(
        "--scale-factors", 
        nargs="+", 
        type=int, 
        default=[1, 10], 
        help="Scale factors in GB (default: 1 10)"
    )
    parser.add_argument(
        "--formats", 
        nargs="+", 
        default=FORMATS, 
        help=f"Data formats (default: {' '.join(FORMATS)})"
    )
    
    # Simple-auth HDFS options
    parser.add_argument(
        "--simple-auth-hadoop-conf", 
        required=True, 
        help="Path to Hadoop configuration for Simple-auth cluster"
    )
    parser.add_argument("--simple-auth-user", default="hdfs", help="Hadoop user name")
    
    # Kerberized HDFS options
    parser.add_argument(
        "--kerberized-hadoop-conf", 
        required=True, 
        help="Path to Hadoop configuration for Kerberized cluster"
    )
    parser.add_argument("--keytab", required=True, help="Path to keytab file")
    parser.add_argument("--principal", required=True, help="Kerberos principal")
    
    # Add requirements check argument
    parser.add_argument("--check-requirements", action="store_true", 
                      help="Check Python requirements for HDFS operations")
    
    # Skip Kerberized HDFS upload
    parser.add_argument("--skip-kerberized", action="store_true",
                      help="Skip upload to Kerberized HDFS")
                      
    # Skip Simple-auth HDFS upload
    parser.add_argument("--skip-simple-auth", action="store_true",
                      help="Skip upload to Simple-auth HDFS")
    
    args = parser.parse_args()
    
    # Check Python requirements if requested
    if args.check_requirements:
        missing_deps = []
        optional_deps = []
        
        # Check for PyArrow (primary HDFS client)
        try:
            import pyarrow.hdfs
            logger.info("✓ pyarrow.hdfs is available - preferred HDFS client")
        except ImportError:
            missing_deps.append("pyarrow")
            
            # Check alternatives
            try:
                import pydoop.hdfs
                logger.info("✓ pydoop.hdfs is available - alternative HDFS client")
            except ImportError:
                try:
                    import hdfs
                    logger.info("✓ hdfs is available - alternative HDFS client")
                except ImportError:
                    missing_deps.append("pydoop or hdfs")
        
        # Check for Kerberos libraries
        kerberos_available = False
        try:
            import gssapi
            logger.info("✓ gssapi is available - preferred for Kerberos authentication")
            kerberos_available = True
        except ImportError:
            optional_deps.append("gssapi")
            
            try:
                import kerberos
                logger.info("✓ kerberos is available - alternative for Kerberos authentication")
                kerberos_available = True
            except ImportError:
                optional_deps.append("kerberos")
        
        # Check for requests-kerberos (used with hdfs client)
        try:
            import requests_kerberos
            logger.info("✓ requests-kerberos is available - for HTTP Kerberos auth")
        except ImportError:
            optional_deps.append("requests-kerberos")
        
        # Output dependency status
        if missing_deps:
            logger.error("⚠️ Missing required dependencies:")
            install_cmds = []
            for dep in missing_deps:
                install_cmds.append(f"pip install {dep}")
            logger.error("Run one of these commands to install a required dependency:")
            for cmd in install_cmds:
                logger.error(f"  {cmd}")
            return False
        
        if optional_deps and not kerberos_available:
            logger.warning("⚠️ Missing Kerberos authentication dependencies:")
            logger.warning("For Kerberized HDFS support, install one of these:")
            for dep in optional_deps:
                logger.warning(f"  pip install {dep}")
        
        logger.info("✓ All critical dependencies are available")
        return True
    
    logger.info("Starting TPC-DS data upload to HDFS clusters...")
    
    for scale in args.scale_factors:
        for format_type in args.formats:
            logger.info(f"Processing {scale}GB {format_type} data...")
            
            # Upload to Simple-auth HDFS if not skipped
            if not args.skip_simple_auth:
                simple_auth_success = upload_to_simple_auth_hdfs(
                    args.data_dir, 
                    args.hdfs_target_dir, 
                    scale, 
                    format_type,
                    args.simple_auth_hadoop_conf,
                    args.simple_auth_user
                )
                
                if not simple_auth_success:
                    logger.error(f"Failed to upload {scale}GB {format_type} data to Simple-auth HDFS")
                    return False
            
            # Upload to Kerberized HDFS if not skipped
            if not args.skip_kerberized:
                kerberized_success = upload_to_kerberized_hdfs(
                    args.data_dir,
                    args.hdfs_target_dir,
                    scale,
                    format_type,
                    args.kerberized_hadoop_conf,
                    args.keytab,
                    args.principal
                )
                
                if not kerberized_success:
                    logger.error(f"Failed to upload {scale}GB {format_type} data to Kerberized HDFS")
                    return False
    
    logger.info("TPC-DS data upload completed successfully")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 