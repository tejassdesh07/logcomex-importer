#!/usr/bin/env python3
"""
Performance Test Script for Logcomex Importer API
Tests the speed improvements of the optimized server
"""

import asyncio
import aiohttp
import time
import json
from concurrent.futures import ThreadPoolExecutor

API_BASE_URL = "http://localhost:8000"

async def test_endpoint(session, endpoint, method="GET", data=None):
    """Test a single endpoint and measure response time"""
    start_time = time.time()
    
    if method == "GET":
        async with session.get(f"{API_BASE_URL}{endpoint}") as response:
            result = await response.json()
            response_time = time.time() - start_time
            return endpoint, response.status, response_time, result
    else:
        async with session.post(f"{API_BASE_URL}{endpoint}", json=data) as response:
            result = await response.json()
            response_time = time.time() - start_time
            return endpoint, response.status, response_time, result

async def concurrent_test(endpoints, max_concurrent=10):
    """Test multiple endpoints concurrently"""
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def limited_test(endpoint_data):
            async with semaphore:
                endpoint, method, data = endpoint_data
                return await test_endpoint(session, endpoint, method, data)
        
        tasks = [limited_test(endpoint_data) for endpoint_data in endpoints]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return results

def analyze_results(results):
    """Analyze and display performance results"""
    print("\n" + "="*70)
    print("PERFORMANCE TEST RESULTS")
    print("="*70)
    
    total_requests = len(results)
    successful_requests = 0
    total_time = 0
    fastest_time = float('inf')
    slowest_time = 0
    
    for result in results:
        if isinstance(result, Exception):
            print(f"❌ Error: {result}")
            continue
            
        endpoint, status, response_time, data = result
        total_time += response_time
        fastest_time = min(fastest_time, response_time)
        slowest_time = max(slowest_time, response_time)
        
        if status == 200:
            successful_requests += 1
            status_icon = "✅"
        else:
            status_icon = "❌"
            
        print(f"{status_icon} {endpoint:<25} | {status:3d} | {response_time:6.3f}s")
    
    print("\n" + "-"*70)
    print(f"📊 SUMMARY:")
    print(f"   Total Requests: {total_requests}")
    print(f"   Successful: {successful_requests}")
    print(f"   Failed: {total_requests - successful_requests}")
    print(f"   Success Rate: {(successful_requests/total_requests*100):.1f}%")
    print(f"   Average Response Time: {(total_time/total_requests):.3f}s")
    print(f"   Fastest Response: {fastest_time:.3f}s")
    print(f"   Slowest Response: {slowest_time:.3f}s")
    print("="*70)

async def load_test():
    """Perform load testing"""
    print("🚀 Starting Performance Tests...")
    
    # Test endpoints
    test_cases = [
        ("/ping", "GET", None),    # New fast endpoint
        ("/", "GET", None),
        ("/status", "GET", None),
        ("/health", "GET", None),
        ("/ping", "GET", None),    # Test again
        ("/status", "GET", None),  # Test again
        ("/", "GET", None),        # Test again
    ]
    
    # Repeat test cases to simulate load
    extended_test_cases = test_cases * 20  # 100 total requests
    
    print(f"📈 Running {len(extended_test_cases)} concurrent requests...")
    
    start_time = time.time()
    results = await concurrent_test(extended_test_cases, max_concurrent=20)
    total_test_time = time.time() - start_time
    
    analyze_results(results)
    
    print(f"\n🎯 LOAD TEST RESULTS:")
    print(f"   Total Test Duration: {total_test_time:.3f}s")
    print(f"   Requests per Second: {len(extended_test_cases)/total_test_time:.2f}")
    print(f"   Concurrent Users Simulated: 20")

async def import_test():
    """Test import functionality"""
    print("\n🔄 Testing Import Functionality...")
    
    import_data = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "importer_name": "DANFOSS INDUSTRIES SA DE CV",
        "clear_existing": False,
        "run_summarize": False
    }
    
    async with aiohttp.ClientSession() as session:
        start_time = time.time()
        
        try:
            async with session.post(f"{API_BASE_URL}/import", json=import_data) as response:
                result = await response.json()
                response_time = time.time() - start_time
                
                if response.status == 200:
                    print(f"✅ Import completed successfully in {response_time:.3f}s")
                    print(f"   Records fetched: {result.get('records_fetched', 0)}")
                    print(f"   Records inserted: {result.get('records_inserted', 0)}")
                    print(f"   Total records: {result.get('total_records', 0)}")
                else:
                    print(f"❌ Import failed: {response.status} - {result}")
                    
        except Exception as e:
            print(f"❌ Import test failed: {e}")

def main():
    """Main test function"""
    print("🎯 LOGCOMEX IMPORTER PERFORMANCE TEST")
    print("This script tests the optimized FastAPI server performance")
    print("Make sure the server is running on localhost:8000")
    
    try:
        # Run load test
        asyncio.run(load_test())
        
        # Test import functionality (optional)
        print("\n" + "="*70)
        choice = input("Run import functionality test? (y/N): ").strip().lower()
        if choice == 'y':
            asyncio.run(import_test())
        
        print("\n🎉 Performance testing completed!")
        print("💡 Check server logs for detailed performance metrics")
        
    except KeyboardInterrupt:
        print("\n❌ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")

if __name__ == "__main__":
    main()
