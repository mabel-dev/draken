from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import List
from typing import Tuple

import opteryx


class MutablePartialAggregate:
    def __init__(
        self,
        count: int = 0,
        sum: float = 0.0,
        min_: float = float("inf"),
        max_: float = float("-inf"),
        missing: int = 0,
    ):
        self.count = count
        self.sum = sum
        self.min = min_
        self.max = max_
        self.missing = missing


class PartialAggregate:
    def __init__(self, count: int, sum: float, min_: float, max_: float, missing: int):
        self.count = count
        self.sum = sum
        self.min = min_
        self.max = max_
        self.missing = missing


class Aggregate:
    def __init__(self, count: int, sum: float, avg: float, min_: float, max_: float, missing: int):
        self.count = count
        self.sum = sum
        self.avg = avg
        self.min = min_
        self.max = max_
        self.missing = missing


class AggregationFramework:
    def lift(self, input_data: float) -> MutablePartialAggregate:
        return MutablePartialAggregate(
            count=1, sum=input_data, min_=input_data, max_=input_data, missing=input_data
        )

    def combine_mutable(self, mutable: MutablePartialAggregate, input_data: float) -> None:
        mutable.count += 1
        if input_data is None:
            mutable.missing += 1
        else:
            mutable.sum += input_data
            mutable.min = min(mutable.min, input_data)
            mutable.max = max(mutable.max, input_data)

    def freeze(self, mutable: MutablePartialAggregate) -> PartialAggregate:
        return PartialAggregate(
            count=mutable.count,
            sum=mutable.sum,
            min_=mutable.min,
            max_=mutable.max,
            missing=mutable.missing,
        )

    def combine(self, a: PartialAggregate, b: PartialAggregate) -> PartialAggregate:
        return PartialAggregate(
            count=a.count + b.count,
            sum=a.sum + b.sum,
            min_=min(a.min, b.min),
            max_=max(a.max, b.max),
            missing=a.missing + b.missing,
        )

    def lower(self, partial: PartialAggregate) -> Aggregate:
        avg = partial.sum / partial.count if partial.count != 0 else 0
        return Aggregate(
            count=partial.count,
            sum=partial.sum,
            avg=avg,
            min_=partial.min,
            max_=partial.max,
            missing=partial.missing,
        )


class HierarchicalAggregateWheel:
    def __init__(self):
        self.seconds = defaultdict(MutablePartialAggregate)
        self.minutes = defaultdict(MutablePartialAggregate)
        self.hours = defaultdict(MutablePartialAggregate)
        self.days = defaultdict(MutablePartialAggregate)
        self.weeks = defaultdict(MutablePartialAggregate)
        self.years = defaultdict(MutablePartialAggregate)

    def aggregate(self, timestamp: datetime, value: float, framework: AggregationFramework):
        # Aggregate by second
        second_key = timestamp.replace(microsecond=0)
        if second_key not in self.seconds:
            self.seconds[second_key] = MutablePartialAggregate()
        framework.combine_mutable(self.seconds[second_key], value)

        # Aggregate by minute
        minute_key = second_key.replace(second=0)
        if minute_key not in self.minutes:
            self.minutes[minute_key] = MutablePartialAggregate()
        framework.combine_mutable(self.minutes[minute_key], value)

        # Aggregate by hour
        hour_key = minute_key.replace(minute=0)
        if hour_key not in self.hours:
            self.hours[hour_key] = MutablePartialAggregate()
        framework.combine_mutable(self.hours[hour_key], value)

        # Aggregate by day
        day_key = hour_key.replace(hour=0)
        if day_key not in self.days:
            self.days[day_key] = MutablePartialAggregate()
        framework.combine_mutable(self.days[day_key], value)

        # Aggregate by week
        week_key = day_key - timedelta(days=day_key.weekday())
        if week_key not in self.weeks:
            self.weeks[week_key] = MutablePartialAggregate()
        framework.combine_mutable(self.weeks[week_key], value)

        # Aggregate by year
        year_key = day_key.replace(month=1, day=1)
        if year_key not in self.years:
            self.years[year_key] = MutablePartialAggregate()
        framework.combine_mutable(self.years[year_key], value)

    def freeze(self, framework: AggregationFramework):
        self.seconds = {k: framework.freeze(v) for k, v in self.seconds.items()}
        self.minutes = {k: framework.freeze(v) for k, v in self.minutes.items()}
        self.hours = {k: framework.freeze(v) for k, v in self.hours.items()}
        self.days = {k: framework.freeze(v) for k, v in self.days.items()}
        self.weeks = {k: framework.freeze(v) for k, v in self.weeks.items()}
        self.years = {k: framework.freeze(v) for k, v in self.years.items()}

    def combine_wheels(self, other, framework: AggregationFramework):
        self.seconds = self._combine_dicts(self.seconds, other.seconds, framework)
        self.minutes = self._combine_dicts(self.minutes, other.minutes, framework)
        self.hours = self._combine_dicts(self.hours, other.hours, framework)
        self.days = self._combine_dicts(self.days, other.days, framework)
        self.weeks = self._combine_dicts(self.weeks, other.weeks, framework)
        self.years = self._combine_dicts(self.years, other.years, framework)

    def _combine_dicts(self, dict_a, dict_b, framework: AggregationFramework):
        combined = defaultdict(lambda: PartialAggregate(0, 0.0))
        for k, v in dict_a.items():
            combined[k] = v
        for k, v in dict_b.items():
            if k in combined:
                combined[k] = framework.combine(combined[k], v)
            else:
                combined[k] = v
        return combined


class µWheelIndex:
    def __init__(self, framework: AggregationFramework):
        self.framework = framework
        self.haw = HierarchicalAggregateWheel()

    def build_index_for_file(self, data: List[Tuple[datetime, float]]):
        for timestamp, value in data:
            self.haw.aggregate(timestamp, value, self.framework)
        self.haw.freeze(self.framework)
        return self.haw

    def combine_indices(self, haw_a: HierarchicalAggregateWheel, haw_b: HierarchicalAggregateWheel):
        haw_a.combine_wheels(haw_b, self.framework)
        return haw_a

    def query(self, granularity: str, start: datetime, end: datetime):
        aggregates = []
        if granularity == "second":
            aggregates = self._query_range(self.haw.seconds, start, end)
        elif granularity == "minute":
            aggregates = self._query_range(self.haw.minutes, start, end)
        elif granularity == "hour":
            aggregates = self._query_range(self.haw.hours, start, end)
        elif granularity == "day":
            aggregates = self._query_range(self.haw.days, start, end)
        elif granularity == "week":
            aggregates = self._query_range(self.haw.weeks, start, end)
        elif granularity == "year":
            aggregates = self._query_range(self.haw.years, start, end)
        return aggregates

    def _query_range(self, wheel: Dict[datetime, PartialAggregate], start: datetime, end: datetime):
        results = []
        for timestamp, aggregate in wheel.items():
            if start <= timestamp <= end:
                results.append((timestamp, self.framework.lower(aggregate)))
        return results


data_file1 = zip(
    *opteryx.query(
        "select Launched_at, Price from $missions WHERE Launched_at is not null"
    ).collect([0, 1])
)


# Example usage
framework = AggregationFramework()
index_service = µWheelIndex(framework)


haw1 = index_service.build_index_for_file(data_file1)

# combined_haw = index_service.combine_indices(haw1, haw2)

# Querying the combined HAW
start_time = datetime(1960, 5, 15, 12, 0, 0)
end_time = datetime(2024, 5, 15, 12, 59, 59)
results = index_service.query("hour", start_time, end_time)

for timestamp, aggregate in results:
    print(
        f"Timestamp: {timestamp}, Count: {aggregate.count}, Sum: {aggregate.sum}, Avg: {aggregate.avg}, Min: {aggregate.min}, Max: {aggregate.max}, missing: {aggregate.missing}"
    )
