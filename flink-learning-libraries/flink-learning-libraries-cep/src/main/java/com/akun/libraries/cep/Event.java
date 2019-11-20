package com.akun.libraries.cep;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author akun
 * @date 2019/11/20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Event {
    private Integer id;
    private String name;
}
