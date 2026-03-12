from src.monitoring.metrics import MonitoringMetrics
from datetime import datetime
import json

metrics = MonitoringMetrics()
data = metrics.compute_all()

report = f"""
# Rapport Monitoring CheckIT_AI - {datetime.now().strftime('%Y-%m-%d %H:%I')}

## KPIs Critiques

- Articles/jour: {data['articles_by_source']['count'].sum()}
- % avec images: {data['articles_with_images']['pct']}%
- % Doublons: {data['duplicates']['dup_pct']}%

...
"""

with open(f'reports/monitoring_{datetime.now().strftime("%Y%m%d")}.md', 'w') as f:
    f.write(report)