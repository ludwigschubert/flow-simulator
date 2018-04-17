""" Vision Models > Evaluations > Feature Inversions

Computes feature inversions (https://arxiv.org/abs/1412.0035).

Also shows off how task specifications get evaluated at parse-time.
"""

output = ("/data/model-evaluations/feature-inversion/"
          "{example_image}/{model}/{layer}.png")

example_image = ["/data/example-images/dog_cat.jpg"]
model = ["googlenet"]
layer = ['conv2d{}'.format(i) for i in range(3)] + \
        ['mixed3a', 'mixed3b',
         'mixed4a', 'mixed4b', 'mixed4c', 'mixed4d', 'mixed4e',
         'mixed5a', 'mixed5b']

def main() -> None:
  # Imports
  import tensorflow as tf
  import numpy as np
  import lucid.modelzoo.vision_models as models
  from lucid.misc.io import show, load, save
  import lucid.optvis.objectives as objectives
  import lucid.optvis.param as param
  import lucid.optvis.render as render
  import lucid.optvis.transform as transform

  @objectives.wrap_objective
  def dot_compare(layer, batch=1, cossim_pow=0.5, epsilon=1e-6):
    def inner(T):
      dot = tf.reduce_sum(T(layer)[batch] * T(layer)[0])
      mag = tf.sqrt(tf.reduce_sum(T(layer)[0]**2))
      cossim = dot/(epsilon + mag)
      return dot * cossim ** cossim_pow
    return inner

  def feature_inversion(model, layer, example_image, n_steps=756, cossim_pow=1.0):
    with tf.Graph().as_default(), tf.Session() as sess:
      model_name = type(model).__name__
      img_shape = model.image_shape
      img = example_image # TODO: we'll need to scale at some point
      objective = objectives.Objective.sum([
          dot_compare(layer, cossim_pow=cossim_pow),
          objectives.blur_input_each_step(),
      ])
      t_input = tf.placeholder(tf.float32, img_shape)
      param_f = param.image(img_shape[0])
      param_f = tf.stack([param_f[0], t_input])
      T = render.make_vis_T(model, objective, param_f)
      loss, vis_op, t_image = T("loss"), T("vis_op"), T("input")
      tf.global_variables_initializer().run()
      for i in range(n_steps):
          _ = sess.run([vis_op], {t_input: img})
      return t_image.eval(feed_dict={t_input: img})[0]

  googlenet = models.InceptionV1()
  googlenet.load_graphdef()
  return feature_inversion(googlenet, layer, example_image)
