package kudadiri.data.engineer.security.service;

import kudadiri.data.engineer.domain.entity.User;
import kudadiri.data.engineer.domain.repository.UserRepository;
import kudadiri.data.engineer.security.model.CustomUserDetails;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CurrentUserService {
    private final UserRepository repository;

    public User getCurrentUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication == null
                || !authentication.isAuthenticated()
                || authentication instanceof AnonymousAuthenticationToken
        ) {
            throw new IllegalStateException("User is not authenticated.");
        }

        CustomUserDetails userDetails = (CustomUserDetails) authentication.getPrincipal();

        return repository.findById(userDetails.getUser().getId()).orElseThrow(
                () -> new IllegalStateException("User not found.")
        );
    }
}
