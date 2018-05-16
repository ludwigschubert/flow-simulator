""" Vision Models > Evaluations > Feature Inversions

Computes feature inversions (https://arxiv.org/abs/1412.0035).

Also shows off how task specifications get evaluated at parse-time.
"""

from flow.task_io import load

output = "/data/evaluations/task=feature-inversion/model={model}/example={example}/layer={layer}/objective={objective}/cossim_pow={cossim_pow}/input_blur_coeff={input_blur_coeff}/image.png"

example_image_path = "/data/example-images/{example}.jpg"
model_directory = "/data/models/{model}"
layer = lambda model: [layer['name'] for layer in load("/data/models/{model}/manifest.json".format(model=model))['layers']]
input_blur_coeff = [0., -0.5, -1.]
cossim_pow = [0., 0.5, 1., 2., 4., 8.]
objective = ['dot_compare']

def main() -> None:
  from typing import Any
  # Imports
  import tensorflow as tf
  import numpy as np
  from lucid.modelzoo.vision_base import Model, SerializedModel, FrozenGraphModel
  from lucid.misc.io import show, load, save
  import lucid.optvis.objectives as objectives
  import lucid.optvis.param as param
  import lucid.optvis.render as render
  import lucid.optvis.transform as transform
  import scipy.ndimage as nd

  @objectives.wrap_objective
  def dot_compare(layer, batch=1, cossim_pow=0.5, epsilon=1e-6):
    def inner(T):
      dot = tf.reduce_sum(T(layer)[batch] * T(layer)[0])
      mag = tf.sqrt(tf.reduce_sum(T(layer)[0]**2))
      cossim = dot/(epsilon + mag)
      return dot * cossim ** cossim_pow
    return inner

  def feature_inversion(model, layer, example_image, n_steps=512, cossim_pow=1.0, input_blur_coeff=0.0):
    with tf.Graph().as_default(), tf.Session() as sess:
      model.load_graphdef()
      model_name = type(model).__name__
      img_shape = model.image_shape
      img = example_image
      objective = objectives.Objective.sum([
          dot_compare(layer, cossim_pow=cossim_pow),
          input_blur_coeff * objectives.blur_input_each_step(),
      ])
      t_input = tf.placeholder(tf.float32, img_shape)
      param_f = param.image(img_shape[0])
      param_f = tf.stack([param_f[0], t_input])
      T = render.make_vis_T(model, objective, param_f, transforms=transform.standard_transforms)
      loss, vis_op, t_image = T("loss"), T("vis_op"), T("input")
      tf.global_variables_initializer().run()
      for i in range(n_steps):
          _ = sess.run([vis_op], {t_input: img})
      return t_image.eval(feed_dict={t_input: img})[0]

  model = SerializedModel.from_directory(model_directory)
  example_image = load(example_image_path)
  img_shape = model.image_shape
  image = nd.zoom(example_image, [img_shape[0]/224., img_shape[1]/224., 1.])
  return feature_inversion(model, layer, image, n_steps=512, cossim_pow=cossim_pow, input_blur_coeff=input_blur_coeff)
