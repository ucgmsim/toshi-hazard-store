import logging
import multiprocessing

from toshi_hazard_store.model import openquake_models
from toshi_hazard_store.model.revision_4 import hazard_models

log = logging.getLogger(__name__)

logging.getLogger('pynamodb').setLevel(logging.DEBUG)


# class PyanamodbConsumedHandler(logging.Handler):
#     def __init__(self, level=0) -> None:
#         super().__init__(level)
#         self.consumed = 0

#     def reset(self):
#         self.consumed = 0

#     def emit(self, record):
#         if "pynamodb/connection/base.py" in record.pathname and record.msg == "%s %s consumed %s units":
#             print(record.msg)
#             print(self.consumed)
#             # ('', 'BatchWriteItem', [{'TableName': 'THS_R4_HazardRealizationCurve-TEST_CBC', 'CapacityUnits': 25.0}])
#             if isinstance(record.args[2], list): # # handle batch-write
#                 for itm in record.args[2]:
#                     print(itm)
#                     self.consumed += itm['CapacityUnits']
#             else:
#                 self.consumed += record.args[2]
#             print("CONSUMED:",  self.consumed)

class DynamoBatchWorker(multiprocessing.Process):
    """A worker that batches and saves records to THS

    based on     example 2.
    """

    def __init__(self, task_queue, toshi_id, model, batch_size):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        # self.result_queue = result_queue
        self.toshi_id = toshi_id
        self.model = model
        self.batch_size = batch_size

        # self.pyconhandler = PyanamodbConsumedHandler(logging.DEBUG)
        # log.addHandler(self.pyconhandler)

    def run(self):
        log.info(f"worker {self.name} running with batch size: {self.batch_size}")
        proc_name = self.name
        models = []

        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                log.info('%s: Exiting' % proc_name)
                # finally
                if len(models):
                    self._batch_save(models)
                    log.info(f'Saved final {len(models)} {self.model} models')

                # log.info(f"{self.name} - Total pynamodb operation cost: {self.pyconhandler.consumed} units")
                self.task_queue.task_done()
                break

            assert isinstance(next_task, self.model)
            models.append(next_task)
            if len(models) >= self.batch_size:
                self._batch_save(models)
                models = []
                log.info(f'Saved batch of {self.batch_size} {self.model} models')

            self.task_queue.task_done()
            # self.result_queue.put(answer)

        return

    def _batch_save(self, models):
        # print(f"worker {self.name} saving batch of len: {len(models)}")
        # if self.model == model.ToshiOpenquakeHazardCurveStatsV2:
        #     query.batch_save_hcurve_stats_v2(self.toshi_id, models=models)
        # elif self.model == model.ToshiOpenquakeHazardCurveRlzsV2:
        #     query.batch_save_hcurve_rlzs_v2(self.toshi_id, models=models)
        try:
            if self.model == openquake_models.OpenquakeRealization:
                with openquake_models.OpenquakeRealization.batch_write() as batch:
                    for item in models:
                        batch.save(item)
            elif self.model == hazard_models.HazardRealizationCurve:
                with hazard_models.HazardRealizationCurve.batch_write() as batch:
                    for item in models:
                        batch.save(item)
            else:
                raise ValueError("WHATT!")
        except Exception as err:
            log.error(str(err))
            raise


def save_parallel(toshi_id: str, model_generator, model, num_workers, batch_size=50):
    tasks: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()

    log.info('Creating %d workers' % num_workers)
    workers = [DynamoBatchWorker(tasks, toshi_id, model, batch_size) for i in range(num_workers)]
    for w in workers:
        w.start()

    # Enqueue jobs
    task_count = 0
    for t in model_generator:
        tasks.put(t)
        task_count += 1

    # Add a poison pill for each to signal we've done everything
    for i in range(num_workers):
        tasks.put(None)

    # Wait for all of the tasks to finish
    tasks.join()
    log.info(f'save_parallel completed {task_count} tasks.')
