#!/usr/bin/env python3
"""Update the last-updated timestamp in charts/index.html"""

import os
import re
from datetime import datetime, timezone

def update_timestamp(html_path):
    """Update the timestamp data attribute in the HTML file"""
    
    # Read the current HTML
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Generate ISO 8601 timestamp in UTC
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Check if data-updated attribute already exists
    if 'data-updated=' in html_content:
        # Update existing timestamp
        html_content = re.sub(
            r'data-updated="[^"]*"',
            f'data-updated="{timestamp}"',
            html_content
        )
    else:
        # Add data-updated attribute to the first attribution paragraph
        # We need to add it to just the first occurrence
        def add_data_attr(match):
            # Only add to paragraphs that don't already have data-updated
            if 'data-updated' in match.group(0):
                return match.group(0)
            return match.group(1) + f' data-updated="{timestamp}"' + match.group(2)
        
        html_content = re.sub(
            r'(<p class="attribution")([^>]*>)',
            add_data_attr,
            html_content,
            count=1  # Only replace the first occurrence
        )
    
    # Write the updated HTML
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Updated timestamp in {html_path} to {timestamp}")

if __name__ == "__main__":
    html_path = os.path.join(os.path.dirname(__file__), 'charts', 'index.html')
    update_timestamp(html_path)
