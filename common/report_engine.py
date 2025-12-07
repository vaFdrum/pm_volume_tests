"""
Unified Reporting Engine for Load Testing
Provides enhanced metrics collection, analysis, and reporting capabilities
"""

import json
import csv
import statistics
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
from threading import Lock


class MetricsCollector:
    """
    Enhanced metrics collector with error tracking, SLO monitoring, and percentiles
    """

    def __init__(self, test_name: str):
        self.test_name = test_name
        self.lock = Lock()

        # Test runs data
        self.test_runs: List[Dict] = []
        self.test_start_time: Optional[float] = None
        self.test_end_time: Optional[float] = None

        # Error tracking
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []

        # HTTP request tracking
        self.http_requests: List[Dict] = []

        # External monitors
        self.clickhouse_monitor = None
        self.locust_metrics: Optional[Dict] = None

        # Baseline for comparison
        self.baseline_metrics: Optional[Dict] = None

        # SLO definitions
        self.slo_definitions: Dict[str, Dict] = {}

    def register_test_run(self, metrics: Dict):
        """Register a completed test run"""
        with self.lock:
            self.test_runs.append({
                **metrics,
                'timestamp': datetime.now().isoformat()
            })

    def register_error(self, error: Dict):
        """Register an error occurrence"""
        with self.lock:
            self.errors.append({
                **error,
                'timestamp': datetime.now().isoformat()
            })

    def register_warning(self, warning: Dict):
        """Register a warning"""
        with self.lock:
            self.warnings.append({
                **warning,
                'timestamp': datetime.now().isoformat()
            })

    def register_http_request(self, request: Dict):
        """Register an HTTP request with details"""
        with self.lock:
            self.http_requests.append({
                **request,
                'timestamp': datetime.now().isoformat()
            })

    def set_test_times(self, start_time: float, end_time: Optional[float] = None):
        """Set test start and end times"""
        with self.lock:
            if self.test_start_time is None:
                self.test_start_time = start_time
            if end_time:
                self.test_end_time = end_time

    def set_baseline_metrics(self, baseline: Dict):
        """Set baseline metrics for comparison"""
        with self.lock:
            self.baseline_metrics = baseline

    def define_slo(self, name: str, threshold: float, comparison: str = "less_than"):
        """
        Define an SLO

        Args:
            name: SLO metric name (e.g., "dag1_duration")
            threshold: Threshold value (e.g., 300 for 5 minutes)
            comparison: "less_than" or "greater_than"
        """
        with self.lock:
            self.slo_definitions[name] = {
                'threshold': threshold,
                'comparison': comparison
            }

    def set_clickhouse_monitor(self, monitor):
        """Set ClickHouse monitor instance"""
        with self.lock:
            if self.clickhouse_monitor is None:
                self.clickhouse_monitor = monitor

    def get_statistics(self) -> Dict[str, Any]:
        """Calculate comprehensive statistics from collected metrics"""
        with self.lock:
            if not self.test_runs:
                return {}

            stats = {
                'summary': self._calculate_summary_stats(),
                'performance': self._calculate_performance_stats(),
                'errors': self._calculate_error_stats(),
                'slo_compliance': self._calculate_slo_compliance(),
                'http_stats': self._calculate_http_stats(),
                'user_breakdown': self._calculate_user_breakdown()
            }

            return stats

    def _calculate_summary_stats(self) -> Dict:
        """Calculate summary statistics"""
        total_runs = len(self.test_runs)
        successful_runs = sum(1 for r in self.test_runs if r.get('success', False))
        failed_runs = total_runs - successful_runs

        test_duration = 0
        if self.test_start_time and self.test_end_time:
            test_duration = self.test_end_time - self.test_start_time

        return {
            'test_name': self.test_name,
            'total_runs': total_runs,
            'successful_runs': successful_runs,
            'failed_runs': failed_runs,
            'success_rate': (successful_runs / total_runs * 100) if total_runs > 0 else 0,
            'test_duration_seconds': test_duration,
            'start_time': datetime.fromtimestamp(self.test_start_time).isoformat() if self.test_start_time else None,
            'end_time': datetime.fromtimestamp(self.test_end_time).isoformat() if self.test_end_time else None
        }

    def _calculate_performance_stats(self) -> Dict:
        """Calculate performance metrics with percentiles"""
        metrics = {}

        metric_names = [
            'csv_upload_duration',
            'dag1_duration',
            'dag2_duration',
            'dashboard_duration',
            'total_duration'
        ]

        for metric_name in metric_names:
            values = [r[metric_name] for r in self.test_runs if metric_name in r]

            if values:
                metrics[metric_name] = self._calculate_percentile_stats(values)

                # Add baseline comparison if available
                if self.baseline_metrics and metric_name in self.baseline_metrics:
                    baseline_value = self.baseline_metrics[metric_name]
                    avg_value = metrics[metric_name]['mean']
                    metrics[metric_name]['baseline'] = baseline_value
                    metrics[metric_name]['baseline_diff_percent'] = (
                        ((avg_value - baseline_value) / baseline_value * 100)
                        if baseline_value > 0 else 0
                    )

        return metrics

    def _calculate_percentile_stats(self, values: List[float]) -> Dict:
        """Calculate comprehensive statistics including percentiles"""
        if not values:
            return {}

        sorted_values = sorted(values)

        return {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'mean': statistics.mean(values),
            'median': statistics.median(values),
            'stdev': statistics.stdev(values) if len(values) > 1 else 0,
            'p50': self._percentile(sorted_values, 0.50),
            'p75': self._percentile(sorted_values, 0.75),
            'p90': self._percentile(sorted_values, 0.90),
            'p95': self._percentile(sorted_values, 0.95),
            'p99': self._percentile(sorted_values, 0.99),
            'p999': self._percentile(sorted_values, 0.999)
        }

    def _percentile(self, sorted_values: List[float], percentile: float) -> float:
        """Calculate percentile from sorted values"""
        if not sorted_values:
            return 0.0

        k = (len(sorted_values) - 1) * percentile
        f = int(k)
        c = f + 1

        if c >= len(sorted_values):
            return sorted_values[-1]

        d0 = sorted_values[f] * (c - k)
        d1 = sorted_values[c] * (k - f)
        return d0 + d1

    def _calculate_error_stats(self) -> Dict:
        """Calculate error statistics and categorization"""
        total_errors = len(self.errors)
        total_warnings = len(self.warnings)

        # Categorize errors by type
        error_types = {}
        for error in self.errors:
            error_type = error.get('type', 'Unknown')
            if error_type not in error_types:
                error_types[error_type] = {
                    'count': 0,
                    'endpoints': set(),
                    'retriable': error.get('retriable', False)
                }
            error_types[error_type]['count'] += 1
            if 'endpoint' in error:
                error_types[error_type]['endpoints'].add(error['endpoint'])

        # Convert sets to lists for JSON serialization
        for error_type in error_types:
            error_types[error_type]['endpoints'] = list(error_types[error_type]['endpoints'])

        # Top failing endpoints
        endpoint_errors = {}
        for error in self.errors:
            endpoint = error.get('endpoint', 'Unknown')
            if endpoint not in endpoint_errors:
                endpoint_errors[endpoint] = 0
            endpoint_errors[endpoint] += 1

        top_failing_endpoints = sorted(
            endpoint_errors.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        return {
            'total_errors': total_errors,
            'total_warnings': total_warnings,
            'error_types': error_types,
            'top_failing_endpoints': [
                {'endpoint': ep, 'count': count}
                for ep, count in top_failing_endpoints
            ]
        }

    def _calculate_slo_compliance(self) -> Dict:
        """Calculate SLO compliance for defined SLOs"""
        slo_results = {}

        for slo_name, slo_config in self.slo_definitions.items():
            threshold = slo_config['threshold']
            comparison = slo_config['comparison']

            # Extract values from test runs
            values = [r[slo_name] for r in self.test_runs if slo_name in r]

            if not values:
                continue

            # Calculate compliance
            if comparison == "less_than":
                compliant = sum(1 for v in values if v < threshold)
            else:  # greater_than
                compliant = sum(1 for v in values if v > threshold)

            total = len(values)
            compliance_rate = (compliant / total * 100) if total > 0 else 0

            slo_results[slo_name] = {
                'threshold': threshold,
                'comparison': comparison,
                'compliant_runs': compliant,
                'total_runs': total,
                'compliance_rate': compliance_rate,
                'passed': compliance_rate >= 95.0,  # SLO target: 95% compliance
                'violations': [
                    {'run_index': i, 'value': v}
                    for i, v in enumerate(values)
                    if (comparison == "less_than" and v >= threshold) or
                       (comparison == "greater_than" and v <= threshold)
                ]
            }

        return slo_results

    def _calculate_http_stats(self) -> Dict:
        """Calculate HTTP request statistics"""
        if not self.http_requests:
            return {}

        total_requests = len(self.http_requests)

        # Status code distribution
        status_codes = {}
        for req in self.http_requests:
            status = req.get('status_code', 'unknown')
            status_codes[status] = status_codes.get(status, 0) + 1

        # Method distribution
        methods = {}
        for req in self.http_requests:
            method = req.get('method', 'unknown')
            methods[method] = methods.get(method, 0) + 1

        # Response time stats
        response_times = [req['duration'] for req in self.http_requests if 'duration' in req]

        return {
            'total_requests': total_requests,
            'status_codes': status_codes,
            'methods': methods,
            'response_time_stats': self._calculate_percentile_stats(response_times) if response_times else {}
        }

    def _calculate_user_breakdown(self) -> Dict:
        """Calculate per-user statistics"""
        users = {}

        for run in self.test_runs:
            username = run.get('username', 'unknown')
            if username not in users:
                users[username] = {
                    'runs': [],
                    'successful': 0,
                    'failed': 0
                }

            users[username]['runs'].append(run)
            if run.get('success', False):
                users[username]['successful'] += 1
            else:
                users[username]['failed'] += 1

        # Calculate averages for each user
        for username, data in users.items():
            runs = data['runs']

            for metric in ['csv_upload_duration', 'dag1_duration', 'dag2_duration', 'total_duration']:
                values = [r[metric] for r in runs if metric in r]
                if values:
                    data[f'{metric}_avg'] = sum(values) / len(values)
                    data[f'{metric}_min'] = min(values)
                    data[f'{metric}_max'] = max(values)

        return users


class ReportGenerator:
    """
    Unified report generator supporting multiple output formats
    """

    def __init__(self, collector: MetricsCollector, config: Dict = None):
        self.collector = collector
        self.config = config or {}

    def generate_text_report(self) -> str:
        """Generate comprehensive text report"""
        stats = self.collector.get_statistics()

        lines = []

        # Header
        lines.extend(self._generate_header(stats))

        # Summary
        lines.extend(self._generate_summary_section(stats))

        # Performance metrics
        lines.extend(self._generate_performance_section(stats))

        # Error analysis
        if stats.get('errors', {}).get('total_errors', 0) > 0:
            lines.extend(self._generate_error_section(stats))

        # SLO compliance
        if stats.get('slo_compliance'):
            lines.extend(self._generate_slo_section(stats))

        # HTTP stats
        if stats.get('http_stats'):
            lines.extend(self._generate_http_section(stats))

        # User breakdown
        if stats.get('user_breakdown'):
            lines.extend(self._generate_user_section(stats))

        # ClickHouse metrics
        lines.extend(self._generate_clickhouse_section())

        # Locust metrics
        lines.extend(self._generate_locust_section())

        # Recommendations
        lines.extend(self._generate_recommendations(stats))

        # Footer
        lines.extend(["=" * 80])

        return "\n".join(lines)

    def _generate_header(self, stats: Dict) -> List[str]:
        """Generate report header"""
        summary = stats.get('summary', {})

        return [
            "",
            "=" * 80,
            f"{summary.get('test_name', 'LOAD TEST').upper()} - DETAILED REPORT",
            "=" * 80,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]

    def _generate_summary_section(self, stats: Dict) -> List[str]:
        """Generate summary section"""
        summary = stats.get('summary', {})

        lines = [
            "TEST SUMMARY",
            "-" * 50,
            f"Test Name: {summary.get('test_name', 'N/A')}",
            f"Start Time: {summary.get('start_time', 'N/A')}",
            f"End Time: {summary.get('end_time', 'N/A')}",
            f"Duration: {summary.get('test_duration_seconds', 0):.2f}s ({summary.get('test_duration_seconds', 0)/60:.1f} min)",
            f"Total Runs: {summary.get('total_runs', 0)}",
            f"Successful: {summary.get('successful_runs', 0)} ({summary.get('success_rate', 0):.1f}%)",
            f"Failed: {summary.get('failed_runs', 0)}",
            ""
        ]

        return lines

    def _generate_performance_section(self, stats: Dict) -> List[str]:
        """Generate performance metrics section"""
        perf = stats.get('performance', {})

        lines = [
            "PERFORMANCE METRICS",
            "-" * 50,
        ]

        metric_labels = {
            'csv_upload_duration': 'CSV Upload Time',
            'dag1_duration': 'DAG #1 Duration (ClickHouse Import)',
            'dag2_duration': 'DAG #2 Duration (PM Dashboard)',
            'dashboard_duration': 'Dashboard Load Time',
            'total_duration': 'Total Scenario Duration'
        }

        for metric_name, label in metric_labels.items():
            if metric_name in perf:
                metric_stats = perf[metric_name]

                lines.append(f"\n{label}:")
                lines.append(f"  Count: {metric_stats.get('count', 0)} runs")
                lines.append(f"  Mean: {metric_stats.get('mean', 0):.2f}s")
                lines.append(f"  Median (P50): {metric_stats.get('p50', 0):.2f}s")
                lines.append(f"  Min: {metric_stats.get('min', 0):.2f}s | Max: {metric_stats.get('max', 0):.2f}s")
                lines.append(f"  Std Dev: {metric_stats.get('stdev', 0):.2f}s")
                lines.append(f"  Percentiles:")
                lines.append(f"    P75: {metric_stats.get('p75', 0):.2f}s")
                lines.append(f"    P90: {metric_stats.get('p90', 0):.2f}s")
                lines.append(f"    P95: {metric_stats.get('p95', 0):.2f}s")
                lines.append(f"    P99: {metric_stats.get('p99', 0):.2f}s")

                # Baseline comparison
                if 'baseline' in metric_stats:
                    baseline = metric_stats['baseline']
                    diff = metric_stats.get('baseline_diff_percent', 0)
                    lines.append(f"  Baseline: {baseline:.2f}s | Difference: {diff:+.1f}%")

        lines.append("")
        return lines

    def _generate_error_section(self, stats: Dict) -> List[str]:
        """Generate error analysis section"""
        errors = stats.get('errors', {})

        lines = [
            "ERROR ANALYSIS",
            "-" * 50,
            f"Total Errors: {errors.get('total_errors', 0)}",
            f"Total Warnings: {errors.get('total_warnings', 0)}",
            ""
        ]

        # Error types
        error_types = errors.get('error_types', {})
        if error_types:
            lines.append("Error Types:")
            for error_type, error_data in sorted(error_types.items(), key=lambda x: x[1]['count'], reverse=True):
                retriable = "RETRIABLE" if error_data.get('retriable') else "PERMANENT"
                lines.append(f"  - {error_type}: {error_data['count']} occurrences ({retriable})")
                if error_data.get('endpoints'):
                    lines.append(f"    Affected endpoints: {', '.join(error_data['endpoints'][:3])}")
            lines.append("")

        # Top failing endpoints
        top_failing = errors.get('top_failing_endpoints', [])
        if top_failing:
            lines.append("Top Failing Endpoints:")
            for item in top_failing:
                lines.append(f"  {item['endpoint']}: {item['count']} errors")
            lines.append("")

        return lines

    def _generate_slo_section(self, stats: Dict) -> List[str]:
        """Generate SLO compliance section"""
        slo = stats.get('slo_compliance', {})

        lines = [
            "SLO COMPLIANCE",
            "-" * 50,
        ]

        for slo_name, slo_data in slo.items():
            threshold = slo_data['threshold']
            comparison = slo_data['comparison']
            compliance_rate = slo_data['compliance_rate']
            passed = slo_data['passed']

            status = "✓ PASS" if passed else "✗ FAIL"
            comp_symbol = "<" if comparison == "less_than" else ">"

            lines.append(f"{slo_name} ({comp_symbol} {threshold}): {compliance_rate:.1f}% compliance {status}")
            lines.append(f"  Compliant: {slo_data['compliant_runs']}/{slo_data['total_runs']} runs")

            if slo_data.get('violations'):
                lines.append(f"  Violations: {len(slo_data['violations'])}")

        lines.append("")
        return lines

    def _generate_http_section(self, stats: Dict) -> List[str]:
        """Generate HTTP statistics section"""
        http = stats.get('http_stats', {})

        if not http:
            return []

        lines = [
            "HTTP REQUEST STATISTICS",
            "-" * 50,
            f"Total Requests: {http.get('total_requests', 0)}",
            ""
        ]

        # Status codes
        status_codes = http.get('status_codes', {})
        if status_codes:
            lines.append("Status Codes:")
            for status, count in sorted(status_codes.items()):
                lines.append(f"  {status}: {count}")
            lines.append("")

        # Response times
        rt_stats = http.get('response_time_stats', {})
        if rt_stats:
            lines.append("Response Times:")
            lines.append(f"  Mean: {rt_stats.get('mean', 0):.3f}s")
            lines.append(f"  P50: {rt_stats.get('p50', 0):.3f}s")
            lines.append(f"  P95: {rt_stats.get('p95', 0):.3f}s")
            lines.append(f"  P99: {rt_stats.get('p99', 0):.3f}s")
            lines.append("")

        return lines

    def _generate_user_section(self, stats: Dict) -> List[str]:
        """Generate per-user breakdown section"""
        users = stats.get('user_breakdown', {})

        if not users:
            return []

        lines = [
            "USER BREAKDOWN",
            "-" * 50,
        ]

        for username, user_data in sorted(users.items()):
            lines.append(f"\n{username}:")
            lines.append(f"  Total Runs: {len(user_data['runs'])}")
            lines.append(f"  Successful: {user_data['successful']} | Failed: {user_data['failed']}")

            if 'dag1_duration_avg' in user_data:
                lines.append(f"  DAG #1 avg: {user_data['dag1_duration_avg']:.2f}s "
                           f"(min: {user_data.get('dag1_duration_min', 0):.2f}s, "
                           f"max: {user_data.get('dag1_duration_max', 0):.2f}s)")

            if 'dag2_duration_avg' in user_data:
                lines.append(f"  DAG #2 avg: {user_data['dag2_duration_avg']:.2f}s "
                           f"(min: {user_data.get('dag2_duration_min', 0):.2f}s, "
                           f"max: {user_data.get('dag2_duration_max', 0):.2f}s)")

        lines.append("")
        return lines

    def _generate_clickhouse_section(self) -> List[str]:
        """Generate ClickHouse metrics section"""
        ch_monitor = self.collector.clickhouse_monitor

        if not ch_monitor:
            return [
                "CLICKHOUSE METRICS",
                "-" * 50,
                "[ClickHouse monitoring disabled or unavailable]",
                ""
            ]

        return [ch_monitor.format_summary_report(), ""]

    def _generate_locust_section(self) -> List[str]:
        """Generate Locust metrics section"""
        lm = self.collector.locust_metrics

        if not lm:
            return []

        return [
            "LOCUST STATISTICS",
            "-" * 50,
            f"Total Requests: {lm.get('total_requests', 0):,}",
            f"Total Failures: {lm.get('total_failures', 0):,}",
            f"Failure Rate: {(lm.get('total_failures', 0) / lm.get('total_requests', 1) * 100):.2f}%",
            f"Average RPS: {lm.get('total_rps', 0):.2f} req/s",
            f"Response Time (avg): {lm.get('avg_response_time', 0):.0f} ms",
            f"Response Time (median): {lm.get('median_response_time', 0):.0f} ms",
            f"Response Time (P95): {lm.get('percentile_95', 0):.0f} ms",
            f"Response Time (P99): {lm.get('percentile_99', 0):.0f} ms",
            ""
        ]

    def _generate_recommendations(self, stats: Dict) -> List[str]:
        """Generate smart recommendations based on test results"""
        lines = [
            "RECOMMENDATIONS",
            "-" * 50,
        ]

        recommendations = []

        # Performance recommendations
        perf = stats.get('performance', {})

        # Check for high variance
        for metric_name in ['dag1_duration', 'dag2_duration']:
            if metric_name in perf:
                metric_stats = perf[metric_name]
                mean = metric_stats.get('mean', 0)
                stdev = metric_stats.get('stdev', 0)

                if mean > 0 and (stdev / mean) > 0.3:  # CoV > 30%
                    recommendations.append(
                        f"⚠ High variance in {metric_name} (std dev: {stdev:.2f}s, {stdev/mean*100:.1f}% of mean). "
                        f"Consider investigating performance inconsistency."
                    )

        # Check for baseline degradation
        for metric_name, metric_stats in perf.items():
            if 'baseline_diff_percent' in metric_stats:
                diff = metric_stats['baseline_diff_percent']
                if diff > 50:
                    recommendations.append(
                        f"⚠ {metric_name} is {diff:.1f}% slower than baseline. "
                        f"Exceeds +50% SLA threshold."
                    )
                elif diff > 20:
                    recommendations.append(
                        f"⚡ {metric_name} is {diff:.1f}% slower than baseline. "
                        f"Consider optimization."
                    )

        # Error recommendations
        errors = stats.get('errors', {})
        total_errors = errors.get('total_errors', 0)
        summary = stats.get('summary', {})
        total_runs = summary.get('total_runs', 1)

        if total_errors > 0:
            error_rate = (total_errors / total_runs) * 100
            if error_rate > 5:
                recommendations.append(
                    f"⚠ High error rate: {error_rate:.1f}% ({total_errors} errors in {total_runs} runs). "
                    f"Review error types and failing endpoints."
                )

        # SLO recommendations
        slo = stats.get('slo_compliance', {})
        for slo_name, slo_data in slo.items():
            if not slo_data['passed']:
                compliance = slo_data['compliance_rate']
                recommendations.append(
                    f"⚠ SLO '{slo_name}' not met: {compliance:.1f}% compliance (target: ≥95%). "
                    f"{len(slo_data.get('violations', []))} violations detected."
                )

        # Add recommendations or default message
        if recommendations:
            for rec in recommendations:
                lines.append(f"{rec}\n")
        else:
            lines.append("✓ No critical issues detected. Performance within expected parameters.\n")

        lines.append("")
        return lines

    def generate_json_report(self) -> str:
        """Generate JSON format report"""
        stats = self.collector.get_statistics()

        # Add metadata
        report = {
            'metadata': {
                'report_format': 'json',
                'generated_at': datetime.now().isoformat(),
                'test_name': self.collector.test_name
            },
            'statistics': stats
        }

        return json.dumps(report, indent=2, default=str)

    def generate_csv_report(self) -> str:
        """Generate CSV format report (test runs)"""
        if not self.collector.test_runs:
            return ""

        # Get all unique keys from test runs
        all_keys = set()
        for run in self.collector.test_runs:
            all_keys.update(run.keys())

        fieldnames = sorted(all_keys)

        # Generate CSV
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(self.collector.test_runs)

        return output.getvalue()

    def save_reports(self, output_dir: str = "./logs"):
        """Save reports in all formats"""
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        test_name_clean = self.collector.test_name.lower().replace(' ', '_').replace('-', '_')

        reports = {
            'text': (f"{output_dir}/{test_name_clean}_report_{timestamp}.txt", self.generate_text_report()),
            'json': (f"{output_dir}/{test_name_clean}_report_{timestamp}.json", self.generate_json_report()),
            'csv': (f"{output_dir}/{test_name_clean}_runs_{timestamp}.csv", self.generate_csv_report())
        }

        saved_files = []

        for format_name, (filepath, content) in reports.items():
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                saved_files.append(filepath)
                print(f"[Report] {format_name.upper()} report saved: {filepath}")
            except Exception as e:
                print(f"[Report] Failed to save {format_name} report: {e}")

        return saved_files
