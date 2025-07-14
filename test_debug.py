#!/usr/bin/env python3
"""
Debug script to test response handling
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ndf.download import download
from loguru import logger

def debug_response():
    """Debug the response object"""
    
    # Configure logger
    logger.remove()
    logger.add(sys.stdout, level="DEBUG")
    
    # Create download instance
    d = download()
    
    # Test URL
    test_url = "http://dailyactprod.bgcsef.com/SEF/DailyAct/DailyAct_20250711.xlsx"
    
    logger.info(f"Testing URL: {test_url}")
    
    # Make request directly
    try:
        r = d.session.get(test_url, verify=False)
        logger.info(f"Response object: {r}")
        logger.info(f"Response type: {type(r)}")
        logger.info(f"Response status_code: {r.status_code}")
        logger.info(f"Response status_code type: {type(r.status_code)}")
        logger.info(f"Response url: {r.url}")
        logger.info(f"Response text (first 100 chars): {r.text[:100]}")
        
        # Test conditions step by step
        logger.info(f"r is not None: {r is not None}")
        logger.info(f"r.status_code == 403: {r.status_code == 403}")
        logger.info(f"'Just a moment...' in r.text: {'Just a moment...' in r.text}")
        
        # Test the condition components
        condition1 = r.status_code == 403
        condition2 = "Just a moment..." in r.text
        condition_or = condition1 or condition2
        condition_full = r and condition_or
        
        logger.info(f"condition1 (status == 403): {condition1}")
        logger.info(f"condition2 (Just a moment): {condition2}")
        logger.info(f"condition_or: {condition_or}")
        logger.info(f"condition_full: {condition_full}")
        logger.info(f"condition_full type: {type(condition_full)}")
        
        # Test the exact condition from the code
        if r and r.status_code == 200:
            logger.info("Would return 200 OK")
        elif r and (r.status_code == 403 or "Just a moment..." in r.text):
            logger.info("Would trigger Cloudflare bypass!")
        else:
            logger.warning("Would NOT trigger Cloudflare bypass")
            
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    debug_response() 