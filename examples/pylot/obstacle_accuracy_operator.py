from cv_bridge import CvBridge
import cv2
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import PIL.ImageFont as ImageFont

from carla.image_converter import depth_to_array

from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging

import messages
import detection_utils


class ObstacleAccuracyOperator(Op):

    def __init__(self,
                 name,
                 rgb_camera_setup,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(ObstacleAccuracyOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._bridge = CvBridge()
        self._world_transforms = []
        self._pedestrians = []
        self._vehicles = []
        self._traffic_lights = []
        self._traffic_signs = []
        self._depth_imgs = []
        self._rgb_imgs = []
        (camera_name, pp, img_size, pos) = rgb_camera_setup
        (self._rgb_intrinsic, self._rgb_transform, self._rgb_img_size) = detection_utils.get_camera_intrinsic_and_transform(
            name=camera_name, postprocessing=pp, image_size=img_size, position=pos)

    @staticmethod
    def setup_streams(input_streams, depth_camera_name):
        def is_obstacles_stream(stream):
            return stream.labels.get('obstacles', '') == 'true'

        # XXX(ionel): This methos selects cameras from Carla
        # def is_rgb_camera_stream(stream):
        #     return (stream.labels.get('sensor_type', '') == 'camera' and
        #             stream.labels.get('camera_type', '') == 'SceneFinal')

        def is_ros_transformed_camera_stream(stream):
            return (stream.labels.get('camera', '') == 'true' and
                    stream.labels.get('ros', '') == 'true')

        input_streams.filter_name(depth_camera_name).add_callback(
            ObstacleAccuracyOperator.on_depth_camera_update)
        input_streams.filter(is_ros_transformed_camera_stream).add_callback(
            ObstacleAccuracyOperator.on_rgb_camera_update)
        input_streams.filter_name('world_transform').add_callback(
            ObstacleAccuracyOperator.on_world_transform_update)
        input_streams.filter_name('pedestrians').add_callback(
            ObstacleAccuracyOperator.on_pedestrians_update)
        input_streams.filter_name('vehicles').add_callback(
            ObstacleAccuracyOperator.on_vehicles_update)
        input_streams.filter_name('traffic_lights').add_callback(
            ObstacleAccuracyOperator.on_traffic_lights_update)
        input_streams.filter_name('traffic_signs').add_callback(
            ObstacleAccuracyOperator.on_traffic_signs_update)
        input_streams.filter(is_obstacles_stream).add_callback(
            ObstacleAccuracyOperator.on_obstacles)
        return []

    def on_world_transform_update(self, msg):
        self._world_transforms.append(msg)

    def on_pedestrians_update(self, msg):
        self._pedestrians.append(msg)

    def on_vehicles_update(self, msg):
        self._vehicles.append(msg)

    def on_traffic_lights_update(self, msg):
        self._traffic_lights.append(msg)

    def on_traffic_signs_update(self, msg):
        self._traffic_signs.append(msg)

    def on_depth_camera_update(self, msg):
        self._depth_imgs.append(msg)

    def on_rgb_camera_update(self, msg):
        self._rgb_imgs.append(msg)

    def on_obstacles(self, msg):
        if (len(self._world_transforms) == 0 or
            len(self._pedestrians) == 0 or
            len(self._vehicles) == 0 or
            len(self._traffic_lights) == 0 or
            len(self._traffic_signs) == 0 or
            len(self._depth_imgs) == 0 or
            len(self._rgb_imgs) == 0):
            return

        self._logger.info("Timestamps {} {} {} {} {} {} {}".format(
            self._world_transforms[0].timestamp,
            self._pedestrians[0].timestamp,
            self._vehicles[0].timestamp,
            self._traffic_lights[0].timestamp,
            self._traffic_signs[0].timestamp,
            self._depth_imgs[0].timestamp,
            self._rgb_imgs[0].timestamp))

        timestamp = self._pedestrians[0].timestamp
        world_transform = self._world_transforms[0].data
        self._world_transforms = self._world_transforms[1:]

        # Get the latest RGB and depth images.
        # NOTE: depth_to_array flips the image.
        depth_img = self._depth_imgs[0].data
        depth_array = depth_to_array(depth_img)
        self._depth_imgs = self._depth_imgs[1:]
        image_np = self._bridge.imgmsg_to_cv2(self._rgb_imgs[0].data, 'rgb8')
        rgb_img = Image.fromarray(np.uint8(image_np)).convert('RGB')
        self._rgb_imgs = self._rgb_imgs[1:]

        # Get bboxes for pedestrians.
        pedestrians = self._pedestrians[0].data
        self._pedestrians = self._pedestrians[1:]
        ped_bbox_id = self.__get_pedestrians_bboxes(
            pedestrians, rgb_img, world_transform, depth_array)

        # Get bboxes for vehicles.
        vehicles = self._vehicles[0].data
        self._vehicles = self._vehicles[1:]
        vec_bboxes = self.__get_vehicles_bboxes(
            vehicles, rgb_img, world_transform, depth_array)

        # # Get bboxes for traffic lights.
        # traffic_lights = self._traffic_lights[0].data
        # self._traffic_lights = self._traffic_lights[1:]
        # self.__get_traffic_light_bboxes(traffic_lights, rgb_img,
        #                                 world_transform, depth_array)

        # # Get bboxes for the traffic signs.
        # traffic_signs = self._traffic_signs[0].data
        # self._traffic_signs = self._traffic_signs[1:]
        # self.__get_traffic_sign_bboxes(traffic_signs, rgb_img,
        #                                world_transform, depth_array)

        if self._flags.visualize_ground_obstacles:
            # Draw the image and mark it with the timestamp.
            draw = ImageDraw.Draw(rgb_img)
            draw.text((5, 5),
                      "Timestamp: {}".format(timestamp),
                      fill='black')
            for (pedestrian_id, corners) in ped_bbox_id:
                (xmin, xmax, ymin, ymax) = corners
                draw.rectangle(((xmin, ymin), (xmax, ymax)),
                               width=4,
                               outline='green')
                draw.text((xmin + 1, ymin + 1), str(pedestrian_id))

            for (xmin, xmax, ymin, ymax) in vec_bboxes:
                draw.rectangle(((xmin, ymin), (xmax, ymax)),
                               width=4,
                               outline='blue')
            # Visualize bounding boxes.
            open_cv_image = np.array(rgb_img)
            open_cv_image = open_cv_image[:, :, ::-1].copy()
            cv2.imshow(self.name, open_cv_image)
            cv2.waitKey(1)

    def execute(self):
        self.spin()

    def __compute_area(self, bbox):
        return (bbox[2] - bbox[0] + 1) * (bbox[3] - bbox[1] + 1)

    def __compute_accuracy(self, bbox1, bbox2):
        x1 = max(bbox1[0], bbox2[0])
        y1 = max(bbox1[1], bbox2[1])
        x2 = min(bbox1[2], bbox2[2])
        y2 = min(bbox1[3], bbox2[3])
        intersection_area = max(0, x2 - x1 + 1) * max(0, y2 - y1 + 1)
        bbox1_area = self.__compute_area(bbox1)
        bbox2_area = self.__compute_area(bbox2)
        iou = intersection_area / float(bbox1_area + bbox2_area - intersection_area)
        return iou

    def __get_traffic_light_bboxes(self, traffic_lights, rgb_img,
                                   world_transform, depth_array):
        for (tl_transform, state) in traffic_lights:
            pos = detection_utils.map_ground_3D_transform_to_2D(rgb_img,
                                                                world_transform,
                                                                self._rgb_transform,
                                                                self._rgb_intrinsic,
                                                                self._rgb_img_size,
                                                                tl_transform)
            if pos is not None:
                x = int(pos[0])
                y = int(pos[1])
                z = pos[2].flatten().item(0)
                if detection_utils.have_same_depth(x, y, z, depth_array, 1.0):
                    # TODO(ionel): Figure out bounding box size.
                    detection_utils.add_bounding_box(rgb_img,
                                                     (x - 2, x + 2, y - 2, y + 2),
                                                     color='yellow')

    def __get_traffic_sign_bboxes(self, traffic_signs, rgb_img,
                                  world_transform, depth_array):
        for (ts_transform, speed_sign) in traffic_signs:
            pos = detection_utils.map_ground_3D_transform_to_2D(rgb_img,
                                                                world_transform,
                                                                self._rgb_transform,
                                                                self._rgb_intrinsic,
                                                                self._rgb_img_size,
                                                                ts_transform)
            if pos is not None:
                x = int(pos[0])
                y = int(pos[1])
                z = pos[2].flatten().item(0)
                if detection_utils.have_same_depth(x, y, z, depth_array, 1.0):
                    # TODO(ionel): Figure out bounding box size.
                    detection_utils.add_bounding_box(rgb_img,
                                                     (x - 2, x + 2, y - 2, y + 2),
                                                     color='yellow')

    def __get_pedestrians_bboxes(self, pedestrians, rgb_img, world_transform,
                                 depth_array):
        ped_bbox_id = []
        for (pedestrian_index, pd_transform, bounding_box,
             fwd_speed) in pedestrians:
            bbox = detection_utils.get_2d_bbox_from_3d_box(
                rgb_img, depth_array, world_transform, pd_transform,
                bounding_box, self._rgb_transform, self._rgb_intrinsic,
                self._rgb_img_size, 1.5, 3.0)
            if bbox is not None:
                ped_bbox_id.append((pedestrian_index, bbox))
        return ped_bbox_id

    def __get_vehicles_bboxes(self, vehicles, rgb_img, world_transform,
                              depth_array):
        vec_bboxes = []
        for (vec_transform, bounding_box, fwd_speed) in vehicles:
            bbox = detection_utils.get_2d_bbox_from_3d_box(
                rgb_img, depth_array, world_transform, vec_transform,
                bounding_box, self._rgb_transform, self._rgb_intrinsic,
                self._rgb_img_size, 3.0, 3.0)
            if bbox is not None:
                vec_bboxes.append(bbox)
        return vec_bboxes
