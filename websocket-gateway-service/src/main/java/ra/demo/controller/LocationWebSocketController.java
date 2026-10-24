package ra.demo.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import ra.demo.event.DriverLocationStreamEvent;

@Controller
@RequiredArgsConstructor
@Slf4j
public class LocationWebSocketController {

    private final SimpMessagingTemplate messagingTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;


    // Tài xế stream GPS liên tục (1s/lần) lên endpoint STOMP: /app/location/stream
    @MessageMapping("/location/stream")
    public void receiveDriverLocation(DriverLocationStreamEvent event) {
        if (event == null || event.getDriverId() == null || event.getLat() == null || event.getLng() == null) {
            return;
        }

        log.info("[GPS STREAM 1s] Tài xế #{}: Lat={}, Lng={}, Vehicle={}, TripId={}",
                event.getDriverId(), event.getLat(), event.getLng(), event.getVehicleType(), event.getTripId());

        String driverIdStr = String.valueOf(event.getDriverId());
        String vehicleType = event.getVehicleType() != null ? event.getVehicleType() : "BIKE";

        // 1. Lưu trực tiếp tọa độ vị trí tài xế vào Redis Hash & Redis GEO
        try {
            String lastLocationKey = "driver:location:" + driverIdStr;
            redisTemplate.opsForHash().put(lastLocationKey, "lat", String.valueOf(event.getLat()));
            redisTemplate.opsForHash().put(lastLocationKey, "lng", String.valueOf(event.getLng()));
            redisTemplate.opsForHash().put(lastLocationKey, "vehicleType", vehicleType);

            String status = redisTemplate.opsForValue().get("driver:status:" + driverIdStr);
            // Nếu đang ONLINE hoặc chưa set status -> Cập nhật vị trí vào Redis GEO
            if (status == null || "ONLINE".equalsIgnoreCase(status)) {
                String geoKey = "drivers:available:" + vehicleType;
                redisTemplate.opsForGeo().add(
                        geoKey,
                        new Point(event.getLng(), event.getLat()),
                        driverIdStr
                );
                log.info("[REDIS GEO] Đã lưu tọa độ GPS Tài xế #{} vào Geo Redis [{}] ({}, {})",
                        driverIdStr, geoKey, event.getLat(), event.getLng());
            }
        } catch (Exception e) {
            log.warn("[REDIS UPDATE WARNING] Không thể lưu trực tiếp vào Redis: {}", e.getMessage());
        }

        // 2. Bắn tọa độ qua Kafka Topic "driver-location-telemetry" để Location Service tiêu thụ
        try {
            kafkaTemplate.send("driver-location-telemetry", driverIdStr, event);
        } catch (Exception e) {
            log.warn("[KAFKA SEND WARNING] Không thể gửi tới Kafka topic driver-location-telemetry: {}", e.getMessage());
        }

        // 3. Nếu tài xế đang trong chuyến đi (tripId != null), forward vị trí realtime cho Khách hàng theo dõi trên bản đồ
        if (event.getTripId() != null) {
            messagingTemplate.convertAndSend("/topic/trip/" + event.getTripId() + "/location", event);
        }
    }
}
