package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Balance;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.entity.UserRecord;

@RestController
public class Controller {
    private final UserRepository userRepository;

    public Controller(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {
        if (userId == null || userId < 0) {
            System.out.println("Invalid user ID: " + userId);
            return new Balance(0f);
        }
        UserRecord user = userRepository.findById(userId).orElse(null);
        if (user == null) {
            System.out.println("User does not exist: " + userId);
            return new Balance(0f);
        }
        return new Balance(user.getBalance());
    }
}
