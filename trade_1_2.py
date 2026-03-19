from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import heapq
from typing import List, Tuple


@dataclass
class Order:
    """
    Represents a single limit order.
    Required attributes:
      - order_id
      - price
      - quantity
      - side ('buy' or 'sell')
      - timestamp
    """
    order_id: int | str
    price: float
    quantity: int
    side: str                 # 'buy' or 'sell'
    timestamp: datetime

    def __post_init__(self) -> None:
        if self.side not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.price <= 0:
            raise ValueError("price must be positive")


class OrderBook:
    """
    Core matching engine.

    Priority rules:
      - Buy side: higher price first, then earlier time
      - Sell side: lower price first, then earlier time

    Implemented with heaps:
      buy_heap:  (-price, timestamp, seq, Order)
      sell_heap: ( price, timestamp, seq, Order)

    NOTE on printed time:
      The assignment's expected output prints the *incoming order's timestamp*
      (the event time that triggers matching)
    """

    def __init__(self) -> None:
        self.buy_heap: List[Tuple[float, datetime, int, Order]] = []
        self.sell_heap: List[Tuple[float, datetime, int, Order]] = []
        self._seq: int = 0  # tie-breaker for identical (price, timestamp)

    def add_order(self, order: Order) -> None:
        """
        Add a new order and immediately attempt to match.
        """
        self._seq += 1
        if order.side == "buy":
            heapq.heappush(self.buy_heap, (-order.price, order.timestamp, self._seq, order))
        else:
            heapq.heappush(self.sell_heap, (order.price, order.timestamp, self._seq, order))

        # Use incoming order's timestamp for all matches triggered by this arrival
        self.match_order(event_time=order.timestamp)

    def match_order(self, event_time: datetime) -> None:
        """
        Execute trades while best buy price >= best sell price.
        For each successful match, print:
          Matched: Buy Order ID X with Sell Order ID Y for quantity Q at time <event_time>
        """
        while self.buy_heap and self.sell_heap:
            best_buy = self.buy_heap[0][3]
            best_sell = self.sell_heap[0][3]

            if best_buy.price < best_sell.price:
                break

            # Pop best orders
            _, _, _, buy_order = heapq.heappop(self.buy_heap)
            _, _, _, sell_order = heapq.heappop(self.sell_heap)

            traded_qty = min(buy_order.quantity, sell_order.quantity)
            buy_order.quantity -= traded_qty
            sell_order.quantity -= traded_qty

            print(
                f"Matched: Buy Order ID {buy_order.order_id} "
                f"with Sell Order ID {sell_order.order_id} "
                f"for quantity {traded_qty} at time {event_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Reinsert partially filled orders
            if buy_order.quantity > 0:
                self._seq += 1
                heapq.heappush(self.buy_heap, (-buy_order.price, buy_order.timestamp, self._seq, buy_order))

            if sell_order.quantity > 0: 
                self._seq += 1
                heapq.heappush(self.sell_heap, (sell_order.price, sell_order.timestamp, self._seq, sell_order))

    def snapshot(self) -> dict:
        """
        Optional helper: return current resting orders (best-to-worst).
        """
        buys = sorted(
            ((-p, ts, o.order_id, o.quantity) for (p, ts, _, o) in self.buy_heap),
            key=lambda x: (-x[0], x[1])
        )
        sells = sorted(
            ((p, ts, o.order_id, o.quantity) for (p, ts, _, o) in self.sell_heap),
            key=lambda x: (x[0], x[1])
        )
        return {"buys": buys, "sells": sells}


def parse_ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    # Example Scenario
    orders = [
        Order(1, 100, 10, "buy",  parse_ts("2023-10-01 10:00:00")),
        Order(2, 102, 5,  "sell", parse_ts("2023-10-01 10:00:01")),
        Order(3, 100, 5,  "sell", parse_ts("2023-10-01 10:00:02")),
        Order(4, 102, 3,  "buy",  parse_ts("2023-10-01 10:00:03")),
        Order(5, 101, 7,  "buy",  parse_ts("2023-10-01 10:00:04")),
        Order(6, 100, 10, "sell", parse_ts("2023-10-01 10:00:05")),
        Order(7, 98,  20, "buy",  parse_ts("2023-10-01 10:00:06")),
        Order(8, 100, 8,  "sell", parse_ts("2023-10-01 10:00:07")),
        Order(9, 100, 5,  "sell", parse_ts("2023-10-01 10:00:08")),
        Order(10, 101, 15, "buy", parse_ts("2023-10-01 10:00:09")),
        Order(11, 100, 25, "sell", parse_ts("2023-10-01 10:00:10")),
        Order(12, 105, 5,  "sell", parse_ts("2023-10-01 10:00:11")),
    ]

    ob = OrderBook()
    for o in orders:
        ob.add_order(o)