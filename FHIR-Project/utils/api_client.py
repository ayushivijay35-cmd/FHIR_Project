import requests
import time
from datetime import datetime
from typing import List, Tuple, Optional


class FHIRAPIClient:
    
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/fhir+json'})
    
    def fetch_with_pagination(
        self, 
        resource_type: str, 
        page_size: int = 50, 
        max_pages: int = 5
    ) -> List[Tuple[dict, str, datetime]]:
        """Fetch FHIR resource with pagination using offset method"""
        results = []
        page_count = 0
        
        print(f"  Fetching {resource_type}...")
        
        url = f"{self.base_url}/{resource_type}"
        offset = 0
        
        while page_count < max_pages:
            try:
                params = {
                    '_count': page_size,
                    '_offset': offset,
                    '_format': 'json'
                }
                
                response = self.session.get(url, params=params, timeout=self.timeout)
                
                if response.status_code != 200:
                    if page_count == 0:
                        print(f"    ✗ Failed to fetch first page: {response.status_code}")
                    else:
                        print(f"    ℹ Stopped at page {page_count} (server returned {response.status_code})")
                    break
                
                data = response.json()
                entries = data.get('entry', [])
                
                # Stop if no entries
                if not entries:
                    if page_count == 0:
                        print(f"     No data available for {resource_type}")
                    else:
                        print(f"     No more data (fetched {page_count} pages)")
                    break
                
                extraction_time = datetime.now()
                results.append((data, response.url, extraction_time))
                page_count += 1
                
                print(f"    ✓ Page {page_count}: {len(entries)} entries")
                
                offset += page_size
                time.sleep(0.5)  # Rate limiting
                
            except requests.exceptions.Timeout:
                print(f"     Timeout at page {page_count + 1}")
                break
            except Exception as e:
                print(f"     Error at page {page_count + 1}: {str(e)[:60]}")
                break
        
        print(f"   Completed: {page_count} pages fetched")
        return results