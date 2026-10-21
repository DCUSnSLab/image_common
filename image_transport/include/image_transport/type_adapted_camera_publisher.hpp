// Copyright 2026 nevlife
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//
//    * Neither the name of the nevlife nor the names of its
//      contributors may be used to endorse or promote products derived from
//      this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#ifndef IMAGE_TRANSPORT__TYPE_ADAPTED_CAMERA_PUBLISHER_HPP_
#define IMAGE_TRANSPORT__TYPE_ADAPTED_CAMERA_PUBLISHER_HPP_

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "rclcpp/node.hpp"
#include "sensor_msgs/msg/camera_info.hpp"

#include "image_transport/camera_common.hpp"
#include "image_transport/type_adapted_publisher.hpp"

namespace image_transport
{

/**
 * \brief CameraPublisher for a type adapted image (ros-perception/image_common#266).
 *
 * The image goes through TypeAdaptedPublisher, the CameraInfo on the info topic that
 * CameraPublisher uses for the same base topic.
 */
template<typename AdapterT>
class TypeAdaptedCameraPublisher
{
public:
  using PublishedType = typename TypeAdaptedPublisher<AdapterT>::PublishedType;

  TypeAdaptedCameraPublisher() = default;

  TypeAdaptedCameraPublisher(
    rclcpp::Node * node,
    const std::string & base_topic,
    rmw_qos_profile_t custom_qos = rmw_qos_profile_default,
    rclcpp::PublisherOptions options = rclcpp::PublisherOptions())
  : image_(node, base_topic, custom_qos, options)
  {
    auto qos = rclcpp::QoS(rclcpp::QoSInitialization::from_rmw(custom_qos), custom_qos);
    info_ = node->create_publisher<sensor_msgs::msg::CameraInfo>(
      getCameraInfoTopic(image_.getTopic()), qos);
  }

  size_t getNumSubscribers() const
  {
    return info_ ? std::max(image_.getNumSubscribers(), info_->get_subscription_count()) : 0;
  }

  std::string getTopic() const
  {
    return image_.getTopic();
  }

  std::string getInfoTopic() const
  {
    return info_ ? info_->get_topic_name() : std::string();
  }

  void publish(
    std::unique_ptr<PublishedType> image,
    sensor_msgs::msg::CameraInfo::UniquePtr info) const
  {
    if (!valid()) {
      return;
    }
    image_.publish(std::move(image));
    info_->publish(std::move(info));
  }

  void publish(const PublishedType & image, const sensor_msgs::msg::CameraInfo & info) const
  {
    if (!valid()) {
      return;
    }
    image_.publish(image);
    info_->publish(info);
  }

  void shutdown()
  {
    image_.shutdown();
    info_.reset();
  }

private:
  bool valid() const
  {
    if (!info_) {
      RCLCPP_FATAL(
        rclcpp::get_logger("image_transport"),
        "Call to publish() on an invalid image_transport::TypeAdaptedCameraPublisher");
      return false;
    }
    return true;
  }

  TypeAdaptedPublisher<AdapterT> image_;
  rclcpp::Publisher<sensor_msgs::msg::CameraInfo>::SharedPtr info_;
};

}  // namespace image_transport

#endif  // IMAGE_TRANSPORT__TYPE_ADAPTED_CAMERA_PUBLISHER_HPP_
