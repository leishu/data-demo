package com.demo.lake.service.batch;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** 批作业编排接口 */
@RestController
@RequestMapping("/api/batch")
public class BatchController {

    private final BatchJobService jobService;

    public BatchController(BatchJobService jobService) {
        this.jobService = jobService;
    }

    public record RunDetail(BatchRun run, List<String> tail) {
    }

    /** 202：作业已提交，进度看 /api/batch/runs/{id} */
    @PostMapping("/{job}")
    public ResponseEntity<BatchRun> submit(@PathVariable String job) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(jobService.submit(job));
    }

    @GetMapping("/runs")
    public List<BatchRun> runs(@RequestParam(defaultValue = "20") int limit) {
        return jobService.list(limit);
    }

    @GetMapping("/runs/{id}")
    public RunDetail detail(@PathVariable String id,
                            @RequestParam(defaultValue = "40") int tail) {
        return new RunDetail(jobService.get(id), jobService.tail(id, tail));
    }
}
