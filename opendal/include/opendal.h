/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


#ifndef _OPENDAL_H
#define _OPENDAL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

typedef struct opendal_reader opendal_reader;

typedef struct opendal_writer opendal_writer;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

struct opendal_writer *opendal_writer(const char *path);

struct opendal_reader *opendal_reader(const char *path);

void opendal_writer_free(struct opendal_writer *writer);

void opendal_reader_free(struct opendal_reader *reader);

intptr_t opendal_writer_write(struct opendal_writer *writer, const uint8_t *data, uintptr_t len);

intptr_t opendal_reader_read(struct opendal_reader *reader, uint8_t *data, uintptr_t len);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* _OPENDAL_H */
