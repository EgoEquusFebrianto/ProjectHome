package kudadiri.data.engineer.data;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.LocalDateTime;

public record Run(
        Integer id,
        @NotNull(message = "Title Must Required")
        String title,
        LocalDateTime startedOn,
        LocalDateTime completedOn,
        @Min(value = 1, message = "Price must be at least one.")
        @Positive
        Integer miles,
        Location location
) {
    public Run {

    }
}
