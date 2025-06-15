import sys
sys.path.append(r"\2023-python-gsg") # Adjust the path as needed

from pmu_producer import send_to_redpanda

# Example usage:
msg = {"id": 1, "content": "Random PMU data"}
send_to_redpanda("orders", msg)