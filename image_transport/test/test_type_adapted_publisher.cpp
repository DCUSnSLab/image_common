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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/image.hpp"

#include "image_transport/image_transport.hpp"

struct AdaptedImage
{
  std::string frame_id;
  uint32_t width = 0;
  uint32_t height = 0;
  std::vector<uint8_t> pixels;
};

std::atomic<int> conversions_to_ros{0};

template<>
struct rclcpp::TypeAdapter<AdaptedImage, sensor_msgs::msg::Image>
{
  using is_specialized = std::true_type;
  using custom_type = AdaptedImage;
  using ros_message_type = sensor_msgs::msg::Image;

  static void convert_to_ros_message(const custom_type & source, ros_message_type & destination)
  {
    ++conversions_to_ros;
    destination.header.frame_id = source.frame_id;
    destination.width = source.width;
    destination.height = source.height;
    destination.encoding = "mono8";
    destination.step = source.width;
    destination.data = source.pixels;
  }

  static void convert_to_custom(const ros_message_type & source, custom_type & destination)
  {
    destination.frame_id = source.header.frame_id;
    destination.width = source.width;
    destination.height = source.height;
    destination.pixels = source.data;
  }
};

using Adapter = rclcpp::TypeAdapter<AdaptedImage, sensor_msgs::msg::Image>;

class TypeAdaptedPublisherTest : public ::testing::Test
{
protected:
  static void SetUpTestSuite() {rclcpp::init(0, nullptr);}
  static void TearDownTestSuite() {rclcpp::shutdown();}

  void SetUp() override
  {
    conversions_to_ros = 0;
  }

  static std::unique_ptr<AdaptedImage> make_image()
  {
    auto image = std::make_unique<AdaptedImage>();
    image->frame_id = "camera";
    image->width = 4;
    image->height = 2;
    image->pixels = {1, 2, 3, 4, 5, 6, 7, 8};
    return image;
  }

  template<typename PredicateT>
  static bool spin_until(rclcpp::Executor & executor, PredicateT predicate)
  {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
      executor.spin_some(std::chrono::milliseconds(10));
    }
    return predicate();
  }
};

TEST_F(TypeAdaptedPublisherTest, intra_process_subscriber_receives_published_object)
{
  auto node = std::make_shared<rclcpp::Node>(
    "adapted_ipc", rclcpp::NodeOptions().use_intra_process_comms(true));
  auto pub = image_transport::create_type_adapted_publisher<Adapter>(node.get(), "camera/image");
  std::unique_ptr<AdaptedImage> received;
  auto sub = image_transport::create_type_adapted_subscription<Adapter>(
    node.get(), "camera/image",
    [&received](std::unique_ptr<AdaptedImage> msg) {received = std::move(msg);});

  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(node);
  ASSERT_TRUE(spin_until(executor, [&pub]() {return pub.getNumSubscribers() == 1u;}));

  auto image = make_image();
  const AdaptedImage * published = image.get();
  pub.publish(std::move(image));
  ASSERT_TRUE(spin_until(executor, [&received]() {return received != nullptr;}));
  EXPECT_EQ(received.get(), published);
  EXPECT_EQ(conversions_to_ros, 0);
  EXPECT_EQ(pub.getTopic(), "/camera/image");
}

TEST_F(TypeAdaptedPublisherTest, external_and_intra_process_subscribers_share_one_conversion)
{
  auto pub_node = std::make_shared<rclcpp::Node>(
    "adapted_pub", rclcpp::NodeOptions().use_intra_process_comms(true));
  auto ext_node = std::make_shared<rclcpp::Node>("external_sub");
  auto pub = image_transport::create_type_adapted_publisher<Adapter>(
    pub_node.get(), "camera/image");

  std::unique_ptr<AdaptedImage> intra_received;
  auto intra_sub = image_transport::create_type_adapted_subscription<Adapter>(
    pub_node.get(), "camera/image",
    [&intra_received](std::unique_ptr<AdaptedImage> msg) {intra_received = std::move(msg);});
  sensor_msgs::msg::Image::ConstSharedPtr ext_received;
  auto ext_sub = image_transport::create_subscription(
    ext_node.get(), "camera/image",
    [&ext_received](const sensor_msgs::msg::Image::ConstSharedPtr & msg) {ext_received = msg;},
    "raw");

  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(pub_node);
  executor.add_node(ext_node);
  ASSERT_TRUE(spin_until(executor, [&pub]() {return pub.getNumSubscribers() == 2u;}));

  auto image = make_image();
  const AdaptedImage * published = image.get();
  pub.publish(std::move(image));
  ASSERT_TRUE(
    spin_until(
      executor, [&]() {return intra_received != nullptr && ext_received != nullptr;}));
  EXPECT_EQ(intra_received.get(), published);
  EXPECT_EQ(conversions_to_ros, 1);
  EXPECT_EQ(ext_received->header.frame_id, "camera");
  EXPECT_EQ(ext_received->data, make_image()->pixels);
}

TEST_F(TypeAdaptedPublisherTest, publishes_by_reference_without_intra_process)
{
  auto pub_node = std::make_shared<rclcpp::Node>("adapted_pub_ref");
  auto ext_node = std::make_shared<rclcpp::Node>("external_sub_ref");
  auto pub = image_transport::create_type_adapted_publisher<Adapter>(
    pub_node.get(), "camera/image_ref");
  sensor_msgs::msg::Image::ConstSharedPtr ext_received;
  auto ext_sub = image_transport::create_subscription(
    ext_node.get(), "camera/image_ref",
    [&ext_received](const sensor_msgs::msg::Image::ConstSharedPtr & msg) {ext_received = msg;},
    "raw");

  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(pub_node);
  executor.add_node(ext_node);
  ASSERT_TRUE(spin_until(executor, [&pub]() {return pub.getNumSubscribers() == 1u;}));

  pub.publish(*make_image());
  ASSERT_TRUE(spin_until(executor, [&ext_received]() {return ext_received != nullptr;}));
  EXPECT_EQ(conversions_to_ros, 1);
  EXPECT_EQ(ext_received->width, 4u);
}

TEST_F(TypeAdaptedPublisherTest, raw_follows_enable_pub_plugins)
{
  auto node = std::make_shared<rclcpp::Node>(
    "adapted_raw_only", rclcpp::NodeOptions().parameter_overrides(
      {rclcpp::Parameter(
        "camera.image_raw_only.enable_pub_plugins",
        std::vector<std::string>{"image_transport/raw"}),
        rclcpp::Parameter(
          "camera.image_no_raw.enable_pub_plugins", std::vector<std::string>{"none"})}));
  auto raw_only = image_transport::create_type_adapted_publisher<Adapter>(
    node.get(), "camera/image_raw_only");
  auto no_raw = image_transport::create_type_adapted_publisher<Adapter>(
    node.get(), "camera/image_no_raw");

  auto publishers = node->get_publishers_info_by_topic("camera/image_raw_only");
  ASSERT_EQ(publishers.size(), 1u);
  EXPECT_EQ(publishers[0].topic_type(), "sensor_msgs/msg/Image");
  EXPECT_EQ(node->count_publishers("camera/image_no_raw"), 0u);
  no_raw.publish(make_image());
  EXPECT_EQ(conversions_to_ros, 0);
}

TEST_F(TypeAdaptedPublisherTest, shutdown_unadvertises_every_copy)
{
  auto node = std::make_shared<rclcpp::Node>("adapted_shutdown");
  auto pub = image_transport::create_type_adapted_publisher<Adapter>(
    node.get(), "camera/image_shutdown");
  auto copy = pub;
  EXPECT_EQ(node->count_publishers("camera/image_shutdown"), 1u);
  pub.shutdown();
  EXPECT_EQ(node->count_publishers("camera/image_shutdown"), 0u);
  EXPECT_EQ(copy.getNumSubscribers(), 0u);
  copy.publish(make_image());
  EXPECT_EQ(conversions_to_ros, 0);
}

TEST_F(TypeAdaptedPublisherTest, camera_publisher_pairs_adapted_image_with_info)
{
  auto pub_node = std::make_shared<rclcpp::Node>(
    "adapted_camera", rclcpp::NodeOptions().use_intra_process_comms(true));
  auto ext_node = std::make_shared<rclcpp::Node>("camera_sub");
  auto pub = image_transport::create_type_adapted_camera_publisher<Adapter>(
    pub_node.get(), "camera/image_cam");
  EXPECT_EQ(pub.getTopic(), "/camera/image_cam");
  EXPECT_EQ(pub.getInfoTopic(), "/camera/camera_info");

  std::unique_ptr<AdaptedImage> intra_received;
  auto intra_sub = image_transport::create_type_adapted_subscription<Adapter>(
    pub_node.get(), "camera/image_cam",
    [&intra_received](std::unique_ptr<AdaptedImage> msg) {intra_received = std::move(msg);});
  sensor_msgs::msg::Image::ConstSharedPtr image_received;
  sensor_msgs::msg::CameraInfo::ConstSharedPtr info_received;
  auto camera_sub = image_transport::create_camera_subscription(
    ext_node.get(), "camera/image_cam",
    [&](const sensor_msgs::msg::Image::ConstSharedPtr & image,
    const sensor_msgs::msg::CameraInfo::ConstSharedPtr & info) {
      image_received = image;
      info_received = info;
    }, "raw");

  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(pub_node);
  executor.add_node(ext_node);
  ASSERT_TRUE(spin_until(executor, [&pub]() {return pub.getNumSubscribers() == 2u;}));

  auto image = make_image();
  const AdaptedImage * published = image.get();
  auto info = std::make_unique<sensor_msgs::msg::CameraInfo>();
  info->header.frame_id = "camera";
  info->width = 4;
  pub.publish(std::move(image), std::move(info));
  ASSERT_TRUE(
    spin_until(
      executor, [&]() {return intra_received != nullptr && image_received != nullptr;}));
  EXPECT_EQ(intra_received.get(), published);
  EXPECT_EQ(conversions_to_ros, 1);
  EXPECT_EQ(image_received->data, make_image()->pixels);
  ASSERT_NE(info_received, nullptr);
  EXPECT_EQ(info_received->width, 4u);
}

TEST_F(TypeAdaptedPublisherTest, raw_and_plugin_subscribers_in_one_process_share_one_conversion)
{
  auto node = std::make_shared<rclcpp::Node>("adapted_with_plugin");
  auto pub = image_transport::create_type_adapted_publisher<Adapter>(
    node.get(), "camera/image_plugin");
  std::unique_ptr<AdaptedImage> raw_received;
  auto raw_sub = image_transport::create_type_adapted_subscription<Adapter>(
    node.get(), "camera/image_plugin",
    [&raw_received](std::unique_ptr<AdaptedImage> msg) {raw_received = std::move(msg);});
  sensor_msgs::msg::Image::ConstSharedPtr plugin_received;
  auto plugin_sub = image_transport::create_subscription(
    node.get(), "camera/image_plugin",
    [&plugin_received](const sensor_msgs::msg::Image::ConstSharedPtr & msg) {
      plugin_received = msg;
    }, "compressed");

  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(node);
  ASSERT_TRUE(spin_until(executor, [&pub]() {return pub.getNumSubscribers() == 2u;}));

  pub.publish(make_image());
  ASSERT_TRUE(
    spin_until(
      executor, [&]() {return raw_received != nullptr && plugin_received != nullptr;}));
  EXPECT_EQ(raw_received->pixels, make_image()->pixels);
  EXPECT_EQ(plugin_received->header.frame_id, "camera");
  EXPECT_EQ(conversions_to_ros, 1);
}
