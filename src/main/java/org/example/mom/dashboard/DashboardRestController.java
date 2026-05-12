package org.example.mom.dashboard;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class DashboardRestController {

    private final DashboardState state;

    public DashboardRestController(DashboardState state) {
        this.state = state;
    }

    @GetMapping("/snapshot")
    public DashboardState.DashboardSnapshot snapshot() {
        return state.snapshot();
    }
}
