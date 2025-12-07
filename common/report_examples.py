"""
Examples of using the enhanced reporting system

This file demonstrates how to integrate the new reporting engine
into your load test scenarios.
"""

from common.report_engine import MetricsCollector, ReportGenerator


def example_basic_usage():
    """
    Basic usage example: collecting metrics and generating report
    """

    # 1. Create a collector for your test
    collector = MetricsCollector(test_name="TC-LOAD-001")

    # 2. Define SLOs
    collector.define_slo("dag1_duration", threshold=300, comparison="less_than")  # < 5 min
    collector.define_slo("dag2_duration", threshold=180, comparison="less_than")  # < 3 min
    collector.define_slo("dashboard_duration", threshold=3, comparison="less_than")  # < 3 sec

    # 3. Set test start time
    import time
    start_time = time.time()
    collector.set_test_times(start_time)

    # 4. Register test runs (this happens in your @task methods)
    collector.register_test_run({
        'success': True,
        'username': 'userNT_1',
        'flow_id': 12345,
        'pm_flow_id': 12346,
        'csv_upload_duration': 45.2,
        'dag1_duration': 240.5,
        'dag2_duration': 165.3,
        'dashboard_duration': 2.1,
        'total_duration': 453.1,
        'file_size': '500 MB',
        'total_lines': 1000000,
        'total_chunks': 125
    })

    # 5. Register errors if they occur
    collector.register_error({
        'type': 'NetworkError',
        'endpoint': 'Upload chunk',
        'message': 'Connection timeout',
        'retriable': True
    })

    # 6. Register HTTP requests for detailed tracking
    collector.register_http_request({
        'method': 'POST',
        'endpoint': '/api/v1/flow/',
        'status_code': 200,
        'duration': 0.245
    })

    # 7. Set test end time
    end_time = time.time()
    collector.set_test_times(start_time, end_time)

    # 8. Generate and save reports
    generator = ReportGenerator(collector)

    # Text report
    text_report = generator.generate_text_report()
    print(text_report)

    # JSON report (for automation/parsing)
    json_report = generator.generate_json_report()

    # CSV report (for Excel analysis)
    csv_report = generator.generate_csv_report()

    # Save all formats
    saved_files = generator.save_reports(output_dir="./logs")
    print(f"Reports saved: {saved_files}")


def example_with_baseline():
    """
    Example with baseline comparison (for TC-LOAD-002, TC-LOAD-003, etc.)
    """

    collector = MetricsCollector(test_name="TC-LOAD-002")

    # Set baseline metrics from TC-LOAD-001
    collector.set_baseline_metrics({
        'csv_upload': 45.0,
        'dag1_duration': 240.0,
        'dag2_duration': 160.0,
        'dashboard_load': 1.5
    })

    # Define SLOs
    collector.define_slo("dag1_duration", threshold=360, comparison="less_than")  # 1.5x baseline

    # ... rest of the test
    # The report will automatically show baseline comparison


def example_integration_with_locust():
    """
    Example of integrating with Locust event listeners
    """

    from locust import events

    # Global collector
    _metrics_collector = MetricsCollector(test_name="MY_TEST")

    @events.test_start.add_listener
    def on_test_start(environment, **kwargs):
        """Initialize collector at test start"""
        import time
        _metrics_collector.set_test_times(time.time())

        # Define your SLOs
        _metrics_collector.define_slo("dag1_duration", 300, "less_than")
        _metrics_collector.define_slo("dag2_duration", 180, "less_than")

    @events.test_stop.add_listener
    def on_test_stop(environment, **kwargs):
        """Generate report at test end"""
        import time
        _metrics_collector.set_test_times(_metrics_collector.test_start_time, time.time())

        # Collect Locust stats
        stats = environment.stats
        _metrics_collector.locust_metrics = {
            'total_rps': stats.total.current_rps if stats.total.num_requests > 0 else 0,
            'total_requests': stats.total.num_requests,
            'total_failures': stats.total.num_failures,
            'median_response_time': stats.total.median_response_time,
            'avg_response_time': stats.total.avg_response_time,
            'percentile_95': stats.total.get_response_time_percentile(0.95),
            'percentile_99': stats.total.get_response_time_percentile(0.99),
        }

        # Generate and save reports
        generator = ReportGenerator(_metrics_collector)
        saved_files = generator.save_reports()

        print(f"\n[Report] Generated {len(saved_files)} report files")


def example_error_tracking():
    """
    Example of comprehensive error tracking
    """

    collector = MetricsCollector(test_name="ERROR_TRACKING_DEMO")

    # Register different types of errors
    collector.register_error({
        'type': 'NetworkError',
        'endpoint': 'Upload chunk 15',
        'message': 'Connection timeout after 30s',
        'retriable': True,
        'status_code': None,
        'retry_attempt': 2
    })

    collector.register_error({
        'type': 'AuthenticationError',
        'endpoint': '/api/v1/security/login',
        'message': 'Invalid credentials',
        'retriable': False,
        'status_code': 401
    })

    collector.register_error({
        'type': 'RateLimitError',
        'endpoint': '/api/v1/flow/',
        'message': 'Rate limit exceeded',
        'retriable': True,
        'status_code': 429
    })

    collector.register_warning({
        'type': 'PerformanceWarning',
        'message': 'DAG #1 duration exceeded P95 threshold',
        'value': 320.5,
        'threshold': 300
    })

    # The report will show:
    # - Total errors by type
    # - Retriable vs permanent errors
    # - Top failing endpoints
    # - Recommendations based on error patterns


def example_percentile_tracking():
    """
    Example showing how percentiles are automatically calculated
    """

    collector = MetricsCollector(test_name="PERCENTILE_DEMO")

    # Simulate multiple test runs with varying performance
    import random
    for i in range(100):
        # Simulate realistic performance variation
        base_dag1 = 240
        variation = random.gauss(0, 30)  # Normal distribution, σ=30s

        collector.register_test_run({
            'success': True,
            'username': f'user_{i % 3}',
            'dag1_duration': max(180, base_dag1 + variation),
            'dag2_duration': max(120, 160 + random.gauss(0, 20)),
            'total_duration': max(300, 500 + random.gauss(0, 50))
        })

    # Generate report - will automatically show:
    # - P50 (median)
    # - P75, P90, P95, P99, P99.9
    # - Standard deviation
    # - Min/Max values

    generator = ReportGenerator(collector)
    print(generator.generate_text_report())


if __name__ == "__main__":
    print("=" * 80)
    print("Enhanced Reporting System - Examples")
    print("=" * 80)
    print("\nExample 1: Basic Usage")
    print("-" * 80)
    example_basic_usage()

    print("\n\nExample 5: Percentile Tracking")
    print("-" * 80)
    example_percentile_tracking()
