package dev.portfolio.inventory.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ReservationRepository extends JpaRepository<Reservation, java.util.UUID> {}
