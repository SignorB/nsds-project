package AccountService;

import org.jetbrains.annotations.NotNull;

/**
 * Represents a user account.
 *
 * @param userId   unique identifier, format "u_xxxxxxxx"
 * @param email    email address, must be unique across all users
 * @param fullName full name
 */
public record User(String userId, String email, String fullName) {

    @Override
    public @NotNull String toString() {
        return "User{userId='" + userId + "', email='" + email + "', fullName='" + fullName + "'}";
    }
}
