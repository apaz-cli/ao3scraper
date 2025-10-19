class RangeSet:
    """Memory-efficient set using sorted ranges for consecutive integers."""
    def __init__(self):
        self.ranges: list[tuple[int, int]] = []

    @classmethod
    def from_values(cls, values: list[int]) -> 'RangeSet':
        """Create RangeSet from list of values efficiently."""
        rs = cls()
        if not values:
            return rs

        sorted_values = sorted(set(values))
        start = sorted_values[0]
        end = sorted_values[0]

        for val in sorted_values[1:]:
            if val == end + 1:
                end = val
            else:
                rs.ranges.append((start, end))
                start = end = val

        rs.ranges.append((start, end))
        return rs

    def add(self, value: int):
        if not self.ranges:
            self.ranges.append((value, value))
            return

        # Binary search for insertion point
        left, right = 0, len(self.ranges)
        while left < right:
            mid = (left + right) // 2
            if self.ranges[mid][1] < value:
                left = mid + 1
            else:
                right = mid

        # Check if already present
        if left < len(self.ranges) and self.ranges[left][0] <= value <= self.ranges[left][1]:
            return

        # Check merge possibilities
        can_merge_prev = left > 0 and self.ranges[left - 1][1] + 1 == value
        can_merge_next = left < len(self.ranges) and self.ranges[left][0] - 1 == value

        if can_merge_prev and can_merge_next:
            new_range = (self.ranges[left - 1][0], self.ranges[left][1])
            self.ranges[left - 1:left + 1] = [new_range]
        elif can_merge_prev:
            self.ranges[left - 1] = (self.ranges[left - 1][0], value)
        elif can_merge_next:
            self.ranges[left] = (value, self.ranges[left][1])
        else:
            self.ranges.insert(left, (value, value))

    def __contains__(self, value: int) -> bool:
        left, right = 0, len(self.ranges)
        while left < right:
            mid = (left + right) // 2
            start, end = self.ranges[mid]
            if value < start:
                right = mid
            elif value > end:
                left = mid + 1
            else:
                return True
        return False

    def __len__(self) -> int:
        return sum(end - start + 1 for start, end in self.ranges)

    def __or__(self, other):
        """Union operation, returns new RangeSet."""
        result = RangeSet()

        if isinstance(other, RangeSet):
            # Merge two sorted lists of ranges
            i, j = 0, 0
            while i < len(self.ranges) or j < len(other.ranges):
                if i >= len(self.ranges):
                    result._add_range(other.ranges[j])
                    j += 1
                elif j >= len(other.ranges):
                    result._add_range(self.ranges[i])
                    i += 1
                elif self.ranges[i][0] <= other.ranges[j][0]:
                    result._add_range(self.ranges[i])
                    i += 1
                else:
                    result._add_range(other.ranges[j])
                    j += 1
        else:
            # Union with regular set
            result.ranges = list(self.ranges)
            for val in other:
                result.add(val)

        return result

    def _add_range(self, new_range: tuple[int, int]):
        """Add a range, merging with last range if adjacent/overlapping."""
        if not self.ranges:
            self.ranges.append(new_range)
            return

        last_start, last_end = self.ranges[-1]
        new_start, new_end = new_range

        if new_start <= last_end + 1:
            # Merge with last range
            self.ranges[-1] = (last_start, max(last_end, new_end))
        else:
            self.ranges.append(new_range)

    def filter_range(self, start: int, end: int) -> list[int]:
        """Return list of values in [start, end] that are NOT in this RangeSet."""
        result = []
        current = start

        # Binary search for first range that could intersect [start, end]
        left, right = 0, len(self.ranges)
        while left < right:
            mid = (left + right) // 2
            if self.ranges[mid][1] < start:
                left = mid + 1
            else:
                right = mid

        # Process ranges starting from the first relevant one
        for i in range(left, len(self.ranges)):
            range_start, range_end = self.ranges[i]
            if range_start > end:
                break

            # Add values before this range
            if current < range_start:
                result.extend(range(current, min(range_start, end + 1)))

            # Skip past this range
            current = max(current, range_end + 1)

        # Add remaining values
        if current <= end:
            result.extend(range(current, end + 1))

        return result

    def discard(self, value: int):
        left, right = 0, len(self.ranges)
        while left < right:
            mid = (left + right) // 2
            start, end = self.ranges[mid]
            if value < start:
                right = mid
            elif value > end:
                left = mid + 1
            else:
                # Found the range containing value
                if start == end:
                    self.ranges.pop(mid)
                elif value == start:
                    self.ranges[mid] = (start + 1, end)
                elif value == end:
                    self.ranges[mid] = (start, end - 1)
                else:
                    self.ranges[mid] = (start, value - 1)
                    self.ranges.insert(mid + 1, (value + 1, end))
                return

    def pop_front(self, count: int) -> list[int]:
        """Remove and return up to count values from the start of the rangeset."""
        result = []
        while result.__len__() < count and self.ranges:
            start, end = self.ranges[0]
            available = min(count - len(result), end - start + 1)
            result.extend(range(start, start + available))

            if available == end - start + 1:
                # Consumed entire first range
                self.ranges.pop(0)
            else:
                # Partial consumption, update range
                self.ranges[0] = (start + available, end)

        return result
