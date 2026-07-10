package kudadiri.data.engineer.dto.response;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BookResponse {
    private Long id;
    private String title;
    private String author;
    private String publisher;
    private Integer publicationYear;
    private Integer stock;
}
