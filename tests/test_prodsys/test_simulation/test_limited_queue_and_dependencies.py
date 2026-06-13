import pytest

import prodsys.express as psx
from prodsys import runner
from prodsys.models import queue_data
from prodsys.simulation import sim, store


def test_reserved_queue_slot_is_released_after_successful_put():
    env = sim.Environment()
    queue = store.Queue(
        env=env,
        data=queue_data.QueueData(ID="limited_queue", description="", capacity=1),
    )

    queue.reserve()
    put_event = queue.put("product")

    assert queue.full
    assert queue._pending_put == 1

    env.run(put_event)

    assert queue.items == ["product"]
    assert queue._pending_put == 0
    assert queue.full

    with pytest.raises(RuntimeError):
        queue.reserve()

    assert queue._pending_put == 0

    env.run(queue.get(lambda item: item == "product"))

    assert not queue.full
    assert queue._pending_put == 0


def test_precedence_dependencies_work_with_link_transport_and_limited_queues():
    time_model_transport = psx.DistanceTimeModel(
        speed=1000, reaction_time=0, ID="tm_transport"
    )
    time_model_process = psx.FunctionTimeModel(
        distribution_function="constant", location=1, ID="tm_process"
    )
    time_model_source = psx.FunctionTimeModel(
        distribution_function="constant", location=50, ID="tm_source"
    )

    link_transport = psx.LinkTransportProcess(
        time_model=time_model_transport, ID="link_transport"
    )
    process_1 = psx.ProductionProcess(time_model_process, "p1")
    process_2 = psx.ProductionProcess(time_model_process, "p2")
    process_3 = psx.ProductionProcess(time_model_process, "p3")

    machine_1 = psx.ProductionResource(
        processes=[process_1], location=[0, 0], internal_queue_size=1, ID="machine_1"
    )
    machine_2 = psx.ProductionResource(
        processes=[process_2], location=[10, 0], internal_queue_size=1, ID="machine_2"
    )
    machine_3 = psx.ProductionResource(
        processes=[process_3], location=[20, 0], internal_queue_size=1, ID="machine_3"
    )
    agv = psx.TransportResource(
        processes=[link_transport], location=[0, 0], ID="agv"
    )

    product = psx.Product(
        processes=[process_1, process_2, process_3],
        transport_process=link_transport,
        ID="product",
    )
    source = psx.Source(
        product=product, time_model=time_model_source, location=[-10, 0], ID="source"
    )
    sink = psx.Sink(product=product, location=[30, 0], ID="sink")

    link_transport.set_links(
        [
            [source, machine_1],
            [source, machine_2],
            [machine_1, machine_2],
            [machine_2, machine_1],
            [machine_1, machine_3],
            [machine_2, machine_3],
            [machine_3, sink],
        ]
    )

    production_system = psx.ProductionSystem(
        resources=[machine_1, machine_2, machine_3, agv],
        sources=[source],
        sinks=[sink],
    )
    adapter = production_system.to_model()
    adapter.product_data[0].processes = {"p1": ["p3"], "p2": ["p3"], "p3": []}

    runner_instance = runner.Runner(adapter=adapter)
    runner_instance.initialize_simulation()
    runner_instance.run(80)

    assert len(runner_instance.product_factory.finished_products) == 1
    finished_product = runner_instance.product_factory.finished_products[0]

    assert set(finished_product.executed_production_processes[:2]) == {"p1", "p2"}
    assert finished_product.executed_production_processes[2] == "p3"


def test_crossing_flows_with_limited_internal_queues_keep_reservations_balanced():
    time_model_transport = psx.FunctionTimeModel(
        distribution_function="constant", location=0.1, ID="tm_transport"
    )
    time_model_process = psx.FunctionTimeModel(
        distribution_function="constant", location=1, ID="tm_process"
    )
    time_model_source = psx.FunctionTimeModel(
        distribution_function="constant", location=2, ID="tm_source"
    )

    transport_process = psx.TransportProcess(time_model_transport, "transport")
    process_1 = psx.ProductionProcess(time_model_process, "p1")
    process_2 = psx.ProductionProcess(time_model_process, "p2")

    machine_1 = psx.ProductionResource(
        processes=[process_1], location=[0, 0], internal_queue_size=1, ID="machine_1"
    )
    machine_2 = psx.ProductionResource(
        processes=[process_2], location=[10, 0], internal_queue_size=1, ID="machine_2"
    )
    agv = psx.TransportResource(
        processes=[transport_process], location=[5, 0], ID="agv"
    )

    product_a = psx.Product(
        processes=[process_1, process_2],
        transport_process=transport_process,
        ID="product_a",
    )
    product_b = psx.Product(
        processes=[process_2, process_1],
        transport_process=transport_process,
        ID="product_b",
    )
    source_a = psx.Source(
        product=product_a, time_model=time_model_source, location=[-5, 0], ID="source_a"
    )
    source_b = psx.Source(
        product=product_b, time_model=time_model_source, location=[15, 0], ID="source_b"
    )
    sink_a = psx.Sink(product=product_a, location=[20, 0], ID="sink_a")
    sink_b = psx.Sink(product=product_b, location=[-10, 0], ID="sink_b")

    production_system = psx.ProductionSystem(
        resources=[machine_1, machine_2, agv],
        sources=[source_a, source_b],
        sinks=[sink_a, sink_b],
    )

    runner_instance = runner.Runner(adapter=production_system.to_model())
    runner_instance.initialize_simulation()
    runner_instance.run(40)

    assert len(runner_instance.product_factory.finished_products) > 0
    for queue in runner_instance.queue_factory.queues:
        assert queue._pending_put >= 0
        assert len(queue.items) + queue._pending_put <= queue.capacity
