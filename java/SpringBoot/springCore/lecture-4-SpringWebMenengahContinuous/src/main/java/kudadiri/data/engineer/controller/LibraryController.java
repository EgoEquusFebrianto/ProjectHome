package kudadiri.data.engineer.controller;

import kudadiri.data.engineer.dto.request.CreateBookRequest;
import kudadiri.data.engineer.dto.request.UpdateBookRequest;
import kudadiri.data.engineer.dto.response.BookResponse;
import kudadiri.data.engineer.service.LibraryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/books")
@RequiredArgsConstructor
public class LibraryController {
    private final LibraryService service;

    @GetMapping
    public List<BookResponse> getAllBooks() {
        return service.getAllBooks();
    }

    @GetMapping("/{id}")
    public BookResponse getBookById(
            @PathVariable Long id) {

        return service.getBookById(id);
    }

    @GetMapping("/search")
    public List<BookResponse> getBookByAuthorName(
            @RequestParam String author) {

        return service.getBookByAuthorName(author);
    }

    @PostMapping
    public BookResponse createBook(
            @RequestBody CreateBookRequest request) {

        return service.createBook(request);
    }

    @PutMapping("/{id}")
    public BookResponse updateBook(
            @PathVariable Long id,
            @RequestBody UpdateBookRequest request) {

        return service.updateBook(id, request);
    }

    @DeleteMapping("/{id}")
    public void deleteBook(
            @PathVariable Long id) {

        service.deleteBook(id);
    }
}
