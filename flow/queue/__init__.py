from flow.queue.enqueuer import Enqueuer
from flow.queue.local import LocalEnqueuer
from flow.queue.gctasks import GCTasksEnqueuer
from flow.queue.gcpulltasks import GCPullTasksEnqueuer

# from flow.queue.gcpubsub import GCPubSubEnqueuer

from absl import flags

FLAGS = flags.FLAGS
flags.DEFINE_boolean(
    "use_local_queue",
    False,
    "Whether to use a local queue for debugging instead of a remote GCTasks queue.",
)


def get_enqueuer() -> Enqueuer:
    # if FLAGS.use_local_queue:
    # return LocalEnqueuer()
    # else:
    return GCPullTasksEnqueuer()
