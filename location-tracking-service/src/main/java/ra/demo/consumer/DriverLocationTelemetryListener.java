package ra.demo.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ra.demo.constants.VehicleType;
import ra.demo.dto.request.DriverLocationRequest;
import ra.demo.event.DriverLocationStreamEvent;
import ra.demo.service.LocationService;

@Component
@RequiredArgsConstructor
@Slf4j
public class DriverLocationTelemetryListener {

    private final LocationService locationService;

    /**
     * Lắng nghe tọa độ GPS liên tục (3s/lần) do websocket-gateway-service bắn sang
     */
    @KafkaListener(
            topics = "driver-location-telemetry",
            groupId = "location-tracking-group"
    )
    public void handleDriverLocationStream(DriverLocationStreamEvent event) {
        if (event == null || event.getDriverId() == null) return;

        log.debug("📥 [KAFKA TELEMETRY] Nhận GPS Tài xế #{}: Lat={}, Lng={}",
                event.getDriverId(), event.getLat(), event.getLng());

        // Chuyển đổi DTO từ Kafka sang Request DTO của LocationService
        DriverLocationRequest request = DriverLocationRequest.builder()
                .driverId(event.getDriverId())
                .latitude(event.getLat())
                .longitude(event.getLng())
                .vehicleType(event.getVehicleType() != null ? VehicleType.valueOf(event.getVehicleType()) : VehicleType.BIKE)
                .build();

        // Gọi LocationService để cập nhật vào Redis Hash & Redis GEO
        locationService.updateDriverLocation(request);
    }
}
