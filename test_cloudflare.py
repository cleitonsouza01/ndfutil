#!/usr/bin/env python3
"""
Test script for Cloudflare bypass functionality
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ndf.download import download
from loguru import logger

def test_cloudflare_bypass():
    """Test the Cloudflare bypass with the BGC URL that was failing"""
    
    # Configure logger to show more details
    logger.remove()
    logger.add(sys.stdout, level="DEBUG")
    
    # Create download instance
    d = download()
    
    # Test the specific URL that was failing
    test_url = "http://dailyactprod.bgcsef.com/SEF/DailyAct/DailyAct_20250711.xlsx"
    
    logger.info(f"Testing Cloudflare bypass with URL: {test_url}")
    
    # Test the download manager directly
    response, error = d.download_manager(test_url)
    
    if response and response.status_code == 200:
        logger.success(f"SUCCESS! Got response with status {response.status_code}")
        logger.info(f"Content length: {len(response.content)} bytes")
        logger.info(f"Content type: {response.headers.get('content-type', 'unknown')}")
        return True
    else:
        logger.error(f"FAILED! Error: {error}")
        return False

def test_bgc_download():
    """Test the full BGC download process"""
    
    logger.info("Testing full BGC download process...")
    
    d = download()
    success, error = d.download_bgc()
    
    if success:
        logger.success("BGC download successful!")
        return True
    else:
        logger.error(f"BGC download failed: {error}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("Testing Cloudflare Bypass")
    print("=" * 60)
    
    # Test 1: Direct URL test
    print("\n1. Testing direct URL download...")
    success1 = test_cloudflare_bypass()
    
    # Test 2: Full BGC download test
    print("\n2. Testing full BGC download...")
    success2 = test_bgc_download()
    
    print("\n" + "=" * 60)
    print("RESULTS:")
    print(f"Direct URL test: {'PASS' if success1 else 'FAIL'}")
    print(f"BGC download test: {'PASS' if success2 else 'FAIL'}")
    print("=" * 60) 