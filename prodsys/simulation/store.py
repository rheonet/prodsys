from __future__ import annotations
from typing import Generator, List, Union
from pydantic import BaseModel


from simpy.resources import store

import logging

logger = logging.getLogger(__name__)

from prodsys.models import queue_data

from prodsys.simulation import sim


class Queue(store.FilterStore):
    """
    Class for storing products in a queue. The queue is a filter store with a limited or unlimited capacity, where product can be put and get from.

    Args:
        env (simpy.Environment): The simulation environment.
        data (data.QueueData): The queue data object.

    Attributes:
        capacity (int, optional): The capacity of the queue. If 0 in the data, the capacity is set to infinity.
        _pending_put (int): The number of products that are reserved for being put into the queue. Avoids bottleneck in the simulation.

    """

    def __init__(self, env: sim.Environment, data: queue_data.QueueData):
        self.env: sim.Environment = env
        self.data: queue_data.QueueData = data
        if data.capacity == 0:
            capacity = float("inf")
        else:
            capacity = data.capacity
        self._pending_put: int = 0
        super().__init__(env, capacity)
        self.state_change = self.env.event()

    def _trigger_state_change(self) -> None:
        """
        Notify waiters that queue contents or reserved capacity changed.
        """
        if not self.state_change.triggered:
            self.state_change.succeed()
        self.state_change = self.env.event()

    def put(self, item) -> Generator:
        """
        Puts a product into the queue.

        Args:
            item (object): The product to be put into the queue.
        """
        return_event = super().put(item)

        def mark_put_complete(_):
            if self._pending_put > 0:
                self.unreserve()
            self._trigger_state_change()

        return_event.callbacks.append(mark_put_complete)
        return return_event

    def get(self, filter) -> Generator:
        """
        Gets a product from the queue.

        Args:
            filter (Callable): The filter function to filter the items in the queue.

        Returns:
            object: The product that was gotten from the queue.
        """
        item = super().get(filter=filter)

        def mark_get_complete(_):
            self._trigger_state_change()

        item.callbacks.append(mark_get_complete)
        return item

    @property
    def full(self) -> bool:
        """
        Checks if the queue is full.

        Returns:
            bool: True if the queue is full, False otherwise.
        """
        logger.debug(
            {
                "ID": self.data.ID,
                "sim_time": self.env.now,
                "event": f"queue has {len(self.items)} items and {self._pending_put} pending puts for capacity {self.capacity}",
            }
        )
        return (self.capacity - self._pending_put - len(self.items)) <= 0

    def reserve(self) -> None:
        """
        Reserves a spot in the queue for a product to be put into.

        Raises:
            RuntimeError: If the queue is full.
        """
        if self.full:
            raise RuntimeError("Queue is full")
        self._pending_put += 1
        logger.debug(
            {
                "ID": self.data.ID,
                "sim_time": self.env.now,
                "event": f"reserving spot in queue {self.data.ID}, current level: {len(self.items)}, pendings: {self._pending_put}",
            }
        )

    def unreserve(self) -> None:
        """
        Unreserves a spot in the queue for a product to be put into after the put is completed.
        """
        if self._pending_put <= 0:
            return
        self._pending_put -= 1


class Store(Queue):
    """
    A store is a storage object for products / auiliaries. It has a location, an input location, and an output location. The input location is the location where products are stored, and the output location is the location where products are retrieved.

    Args:
        env (simpy.Environment): The simulation environment.
        data (data.StoreData): The store data object.
    """

    def __init__(self, env: sim.Environment, data: queue_data.StoreData):
        super().__init__(env, data)
        self.data: queue_data.StoreData = data

    def get_location(self):
        return self.data.location

    def get_input_location(self):
        return self.data.input_location

    def get_output_location(self):
        return self.data.output_location
