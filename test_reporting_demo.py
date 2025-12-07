#!/usr/bin/env python3
"""
Demo script to showcase the enhanced reporting system
"""

import time
import random
from common.report_engine import MetricsCollector, ReportGenerator


def demo_enhanced_reporting():
    """Demonstrate the enhanced reporting capabilities"""

    print("=" * 80)
    print("Enhanced Reporting System - Demo")
    print("=" * 80)
    print()

    # 1. Create collector
    print("1. Creating MetricsCollector...")
    collector = MetricsCollector(test_name="TC-LOAD-DEMO")

    # 2. Define SLOs
    print("2. Defining SLOs...")
    collector.define_slo("dag1_duration", threshold=300, comparison="less_than")
    collector.define_slo("dag2_duration", threshold=180, comparison="less_than")
    collector.define_slo("dashboard_duration", threshold=3, comparison="less_than")
    print("   ✓ Defined 3 SLOs")

    # 3. Set baseline metrics
    print("3. Setting baseline metrics...")
    collector.set_baseline_metrics({
        'csv_upload': 45.0,
        'dag1_duration': 240.0,
        'dag2_duration': 160.0,
        'dashboard_load': 1.5
    })
    print("   ✓ Baseline set")

    # 4. Simulate test execution
    print("4. Simulating test runs...")
    start_time = time.time()
    collector.set_test_times(start_time)

    # Simulate 20 test runs with varying performance
    for i in range(20):
        # Realistic performance variation
        dag1 = 240 + random.gauss(0, 25)  # Mean=240s, σ=25s
        dag2 = 160 + random.gauss(0, 20)  # Mean=160s, σ=20s
        dashboard = 1.5 + random.uniform(0, 1.5)

        # Occasionally simulate slower runs
        if random.random() < 0.1:  # 10% chance
            dag1 *= 1.3
            dag2 *= 1.25

        success = random.random() > 0.05  # 95% success rate

        collector.register_test_run({
            'success': success,
            'username': f'user_{i % 3}',
            'flow_id': 10000 + i,
            'pm_flow_id': 20000 + i,
            'csv_upload_duration': 45 + random.gauss(0, 5),
            'dag1_duration': max(180, dag1),
            'dag2_duration': max(120, dag2),
            'dashboard_duration': max(0.5, dashboard),
            'total_duration': max(300, dag1 + dag2 + 50),
            'file_size': '500 MB',
            'total_lines': 1000000,
            'total_chunks': 125
        })

        # Simulate some errors
        if random.random() < 0.15:  # 15% chance of error
            error_types = [
                ('NetworkError', 'Upload chunk', True),
                ('RateLimitError', 'Create flow', True),
                ('ServiceUnavailableError', 'Start PM flow', True)
            ]
            error_type, endpoint, retriable = random.choice(error_types)

            collector.register_error({
                'type': error_type,
                'endpoint': endpoint,
                'message': f'Simulated {error_type}',
                'retriable': retriable
            })

        # Simulate HTTP requests
        for _ in range(random.randint(5, 15)):
            collector.register_http_request({
                'method': random.choice(['GET', 'POST', 'PUT']),
                'endpoint': random.choice(['/api/v1/flow/', '/api/v1/file/upload', '/etl/api/v1/flow/status/']),
                'status_code': random.choice([200, 200, 200, 201, 429, 503]),
                'duration': random.uniform(0.05, 2.0)
            })

    end_time = time.time()
    collector.set_test_times(start_time, end_time)
    print(f"   ✓ Simulated 20 test runs in {end_time - start_time:.2f}s")

    # 5. Simulate Locust metrics
    print("5. Adding Locust metrics...")
    collector.locust_metrics = {
        'total_requests': 1250,
        'total_failures': 35,
        'total_rps': 12.5,
        'avg_response_time': 450,
        'median_response_time': 380,
        'percentile_95': 890,
        'percentile_99': 1240
    }
    print("   ✓ Locust metrics added")

    # 6. Generate reports
    print("\n6. Generating reports...")
    generator = ReportGenerator(collector)

    # Text report
    print("\n" + "=" * 80)
    print("TEXT REPORT")
    print("=" * 80)
    text_report = generator.generate_text_report()
    print(text_report)

    # Save all formats
    print("\n7. Saving reports to ./logs/...")
    saved_files = generator.save_reports(output_dir="./logs")

    print(f"\n   ✓ Successfully saved {len(saved_files)} reports:")
    for filepath in saved_files:
        print(f"     - {filepath}")

    # Show JSON preview
    print("\n" + "=" * 80)
    print("JSON REPORT (preview - first 50 lines)")
    print("=" * 80)
    json_report = generator.generate_json_report()
    json_lines = json_report.split('\n')
    for line in json_lines[:50]:
        print(line)
    if len(json_lines) > 50:
        print(f"... ({len(json_lines) - 50} more lines)")

    print("\n" + "=" * 80)
    print("Demo completed successfully!")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Check the generated reports in ./logs/")
    print("2. Read REPORTING.md for detailed documentation")
    print("3. See common/report_examples.py for more examples")
    print("4. Integrate into your test scenarios")


if __name__ == "__main__":
    demo_enhanced_reporting()
