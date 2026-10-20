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

#ifndef IMAGE_TRANSPORT__TYPE_ADAPTED_PUBLISHER_HPP_
#define IMAGE_TRANSPORT__TYPE_ADAPTED_PUBLISHER_HPP_

#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "rclcpp/expand_topic_or_service_name.hpp"
#include "rclcpp/logging.hpp"
#include "rclcpp/node.hpp"
#include "rclcpp/publisher.hpp"
#include "rclcpp/type_adapter.hpp"

#include "sensor_msgs/msg/image.hpp"

#include "image_transport/loader_fwds.hpp"
#include "image_transport/visibility_control.hpp"

namespace image_transport
{

namespace detail
{

/// The enabled transports of an image topic except raw.
class PluginPublishers
{
public:
  IMAGE_TRANSPORT_PUBLIC
  PluginPublishers(
    rclcpp::Node * node,
    const std::string & image_topic,
    PubLoaderPtr loader,
    rmw_qos_profile_t custom_qos,
    rclcpp::PublisherOptions options);

  IMAGE_TRANSPORT_PUBLIC
  ~PluginPublishers();

  /// Whether raw is among the enabled transports of the topic.
  IMAGE_TRANSPORT_PUBLIC
  bool rawEnabled() const;

  IMAGE_TRANSPORT_PUBLIC
  size_t getNumSubscribers() const;

  /// Publish on every transport that has subscribers.
  IMAGE_TRANSPORT_PUBLIC
  void publish(const sensor_msgs::msg::Image::ConstSharedPtr & message) const;

  IMAGE_TRANSPORT_PUBLIC
  void shutdown();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

IMAGE_TRANSPORT_PUBLIC
std::shared_ptr<PluginPublishers> createPluginPublishers(
  rclcpp::Node * node,
  const std::string & image_topic,
  rmw_qos_profile_t custom_qos,
  rclcpp::PublisherOptions options);

/// The raw transport publisher of a type adapted image topic.
template<typename AdapterT>
class AdaptedRawPublisher : public rclcpp::Publisher<AdapterT>
{
public:
  using Base = rclcpp::Publisher<AdapterT>;
  using typename Base::PublishedType;
  using typename Base::PublishedTypeDeleter;
  using typename Base::ROSMessageType;

  using Base::Base;

  /// Publish msg, given ros_msg, its conversion already made for the other transports.
  void publish_converted(
    std::unique_ptr<PublishedType, PublishedTypeDeleter> msg,
    const std::shared_ptr<const ROSMessageType> & ros_msg)
  {
    if (!this->intra_process_is_enabled_) {
      this->do_inter_process_publish(*ros_msg);
      return;
    }
    const bool inter_process_publish_needed =
      this->get_subscription_count() > this->get_intra_process_subscription_count() ||
      this->buffer_;
    this->do_intra_process_publish(std::move(msg));
    if (inter_process_publish_needed) {
      this->do_inter_process_publish(*ros_msg);
      if (this->buffer_) {
        this->buffer_->add_shared(ros_msg);
      }
    }
  }

  void publish_converted(
    const PublishedType & msg,
    const std::shared_ptr<const ROSMessageType> & ros_msg)
  {
    if (!this->intra_process_is_enabled_) {
      this->do_inter_process_publish(*ros_msg);
      return;
    }
    publish_converted(this->duplicate_type_adapt_message_as_unique_ptr(msg), ros_msg);
  }
};

}  // namespace detail

/**
 * \brief Publishes a type adapted image on all transports of an image topic.
 *
 * The raw transport publishes the adapted type itself, so intra-process subscribers
 * of the adapted type receive it without conversion. The other transports, and raw
 * subscribers outside the process, share one conversion to sensor_msgs::msg::Image.
 * Publisher plugins and the "<topic>.enable_pub_plugins" parameter work as for Publisher.
 */
template<typename AdapterT>
class TypeAdaptedPublisher
{
public:
  static_assert(
    rclcpp::TypeAdapter<AdapterT>::is_specialized::value,
    "TypeAdaptedPublisher needs an rclcpp::TypeAdapter");
  static_assert(
    std::is_same_v<
      typename rclcpp::TypeAdapter<AdapterT>::ros_message_type, sensor_msgs::msg::Image>,
    "The ROS message type of the adapter must be sensor_msgs::msg::Image");

  using PublishedType = typename rclcpp::TypeAdapter<AdapterT>::custom_type;
  using RawPublisher = detail::AdaptedRawPublisher<AdapterT>;

  TypeAdaptedPublisher() = default;

  TypeAdaptedPublisher(
    rclcpp::Node * node,
    const std::string & base_topic,
    rmw_qos_profile_t custom_qos = rmw_qos_profile_default,
    rclcpp::PublisherOptions options = rclcpp::PublisherOptions())
  : state_(std::make_shared<State>())
  {
    // Resolve the name like Publisher does, so that the transport topics remap together.
    state_->topic = rclcpp::expand_topic_or_service_name(
      base_topic, node->get_name(), node->get_namespace());
    state_->plugins = detail::createPluginPublishers(node, state_->topic, custom_qos, options);
    if (state_->plugins->rawEnabled()) {
      auto qos = rclcpp::QoS(rclcpp::QoSInitialization::from_rmw(custom_qos), custom_qos);
      state_->raw = node->create_publisher<AdapterT, std::allocator<void>, RawPublisher>(
        state_->topic, qos, options);
    }
  }

  size_t getNumSubscribers() const
  {
    if (!state_) {
      return 0;
    }
    return state_->plugins->getNumSubscribers() +
           (state_->raw ? state_->raw->get_subscription_count() : 0);
  }

  std::string getTopic() const
  {
    return state_ ? state_->topic : std::string();
  }

  void publish(std::unique_ptr<PublishedType> message) const
  {
    if (!valid()) {
      return;
    }
    const bool raw_needed = state_->raw && state_->raw->get_subscription_count() > 0;
    if (state_->plugins->getNumSubscribers() == 0) {
      if (raw_needed) {
        state_->raw->publish(std::move(message));
      }
      return;
    }
    auto ros_message = convert(*message);
    state_->plugins->publish(ros_message);
    if (raw_needed) {
      state_->raw->publish_converted(std::move(message), ros_message);
    }
  }

  void publish(const PublishedType & message) const
  {
    if (!valid()) {
      return;
    }
    const bool raw_needed = state_->raw && state_->raw->get_subscription_count() > 0;
    if (state_->plugins->getNumSubscribers() == 0) {
      if (raw_needed) {
        state_->raw->publish(message);
      }
      return;
    }
    auto ros_message = convert(message);
    state_->plugins->publish(ros_message);
    if (raw_needed) {
      state_->raw->publish_converted(message, ros_message);
    }
  }

  /// Shut down the advertisements of every copy of this publisher.
  void shutdown()
  {
    if (state_) {
      state_->plugins->shutdown();
      state_->raw.reset();
      state_.reset();
    }
  }

private:
  struct State
  {
    std::string topic;
    std::shared_ptr<detail::PluginPublishers> plugins;
    std::shared_ptr<RawPublisher> raw;
  };

  bool valid() const
  {
    if (!state_) {
      RCLCPP_FATAL(
        rclcpp::get_logger("image_transport"),
        "Call to publish() on an invalid image_transport::TypeAdaptedPublisher");
      return false;
    }
    return true;
  }

  static std::shared_ptr<const sensor_msgs::msg::Image> convert(const PublishedType & message)
  {
    auto ros_message = std::make_shared<sensor_msgs::msg::Image>();
    rclcpp::TypeAdapter<AdapterT>::convert_to_ros_message(message, *ros_message);
    return ros_message;
  }

  std::shared_ptr<State> state_;
};

}  // namespace image_transport

#endif  // IMAGE_TRANSPORT__TYPE_ADAPTED_PUBLISHER_HPP_
