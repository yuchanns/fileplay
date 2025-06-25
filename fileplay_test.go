package fileplay_test

import (
	"io"
	"os"
	"testing"

	"github.com/google/uuid"
)

var testCreators = map[string]FileCreator{
	"pure":    PureCreator{},
	"ffi":     FFICreator{},
	"opendal": OpenDALCreator{},
}

// TestFileCreateAndClose tests basic file creation and closing
func TestFileCreateAndClose(t *testing.T) {
	for creatorName, creator := range testCreators {
		t.Run(creatorName, func(t *testing.T) {
			t.Parallel()

			path := uuid.NewString()
			t.Cleanup(func() {
				os.Remove(path)
			})

			file, err := creator.Create(path)
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			err = file.Close()
			if err != nil {
				t.Fatalf("Failed to close file: %v", err)
			}
		})
	}
}

// TestFileWrite tests writing data to files
func TestFileWrite(t *testing.T) {
	testData := []byte("Hello, World! This is a test string for file writing.")

	for creatorName, creator := range testCreators {
		t.Run(creatorName, func(t *testing.T) {
			t.Parallel()

			path := uuid.NewString()
			t.Cleanup(func() {
				os.Remove(path)
			})

			file, err := creator.Create(path)
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			bytesWritten, err := file.Write(testData)
			if err != nil {
				t.Fatalf("Failed to write to file: %v", err)
			}

			if bytesWritten != len(testData) {
				t.Fatalf("Expected to write %d bytes, but wrote %d bytes", len(testData), bytesWritten)
			}

			err = file.Close()
			if err != nil {
				t.Fatalf("Failed to close file: %v", err)
			}
		})
	}
}

// TestFileRead tests reading data from files
func TestFileRead(t *testing.T) {
	testData := []byte("Hello, World! This is a test string for file reading.")

	for creatorName, creator := range testCreators {
		t.Run(creatorName, func(t *testing.T) {
			t.Parallel()

			path := uuid.NewString()
			t.Cleanup(func() {
				os.Remove(path)
			})

			// First, create and write test data
			file, err := creator.Create(path)
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			_, err = file.Write(testData)
			if err != nil {
				t.Fatalf("Failed to write test data: %v", err)
			}

			err = file.Close()
			if err != nil {
				t.Fatalf("Failed to close file after writing: %v", err)
			}

			// Then, open and read the data
			file, err = creator.Open(path)
			if err != nil {
				t.Fatalf("Failed to open file for reading: %v", err)
			}

			readData := make([]byte, len(testData))
			bytesRead, err := io.ReadFull(file, readData)
			if err != nil {
				t.Fatalf("Failed to read from file: %v", err)
			}

			if bytesRead != len(testData) {
				t.Fatalf("Expected to read %d bytes, but read %d bytes", len(testData), bytesRead)
			}

			err = file.Close()
			if err != nil {
				t.Fatalf("Failed to close file after reading: %v", err)
			}

			// Verify data consistency
			if string(readData) != string(testData) {
				t.Fatalf("Data mismatch: expected %q, got %q", string(testData), string(readData))
			}
		})
	}
}

// TestFileWriteRead tests the complete write-read cycle for data consistency
func TestFileWriteRead(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"small_text", []byte("Hello, World!")},
		{"empty", []byte("")},
		{"binary_data", genFixedBytes(1024)},
		{"large_text", []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
			"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.")},
	}

	for creatorName, creator := range testCreators {
		t.Run(creatorName, func(t *testing.T) {
			t.Parallel()

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					path := uuid.NewString()
					t.Cleanup(func() {
						os.Remove(path)
					})

					// Write data
					file, err := creator.Create(path)
					if err != nil {
						t.Fatalf("Failed to create file: %v", err)
					}

					bytesWritten, err := file.Write(tc.data)
					if err != nil {
						t.Fatalf("Failed to write data: %v", err)
					}

					if bytesWritten != len(tc.data) {
						t.Fatalf("Expected to write %d bytes, but wrote %d bytes", len(tc.data), bytesWritten)
					}

					err = file.Close()
					if err != nil {
						t.Fatalf("Failed to close file after writing: %v", err)
					}

					// Read data back
					file, err = creator.Open(path)
					if err != nil {
						t.Fatalf("Failed to open file for reading: %v", err)
					}

					readData := make([]byte, len(tc.data))
					if len(tc.data) > 0 {
						bytesRead, err := io.ReadFull(file, readData)
						if err != nil {
							t.Fatalf("Failed to read data: %v", err)
						}

						if bytesRead != len(tc.data) {
							t.Fatalf("Expected to read %d bytes, but read %d bytes", len(tc.data), bytesRead)
						}
					}

					err = file.Close()
					if err != nil {
						t.Fatalf("Failed to close file after reading: %v", err)
					}

					// Verify data consistency
					if string(readData) != string(tc.data) {
						t.Fatalf("Data mismatch: expected %q, got %q", string(tc.data), string(readData))
					}
				})
			}
		})
	}
}

// TestFileOpenNonExistent tests opening non-existent files
func TestFileOpenNonExistent(t *testing.T) {
	for creatorName, creator := range testCreators {
		t.Run(creatorName, func(t *testing.T) {
			t.Parallel()

			nonExistentPath := uuid.NewString() + "_does_not_exist"

			_, err := creator.Open(nonExistentPath)
			if err == nil {
				t.Fatalf("Expected error when opening non-existent file, but got nil")
			}
		})
	}
}

// TestFileMultipleWrites tests multiple write operations to the same file
func TestFileMultipleWrites(t *testing.T) {
	writes := [][]byte{
		[]byte("First write. "),
		[]byte("Second write. "),
		[]byte("Third write."),
	}
	expectedContent := []byte("First write. Second write. Third write.")

	for creatorName, creator := range testCreators {
		t.Run(creatorName, func(t *testing.T) {
			t.Parallel()

			path := uuid.NewString()
			t.Cleanup(func() {
				os.Remove(path)
			})

			// Create file and perform multiple writes
			file, err := creator.Create(path)
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			totalWritten := 0
			for i, data := range writes {
				bytesWritten, err := file.Write(data)
				if err != nil {
					t.Fatalf("Failed to write data in iteration %d: %v", i, err)
				}
				totalWritten += bytesWritten
			}

			err = file.Close()
			if err != nil {
				t.Fatalf("Failed to close file: %v", err)
			}

			// Read back and verify
			file, err = creator.Open(path)
			if err != nil {
				t.Fatalf("Failed to open file for reading: %v", err)
			}

			readData := make([]byte, len(expectedContent))
			bytesRead, err := io.ReadFull(file, readData)
			if err != nil {
				t.Fatalf("Failed to read data: %v", err)
			}

			if bytesRead != len(expectedContent) {
				t.Fatalf("Expected to read %d bytes, but read %d bytes", len(expectedContent), bytesRead)
			}

			err = file.Close()
			if err != nil {
				t.Fatalf("Failed to close file after reading: %v", err)
			}

			if string(readData) != string(expectedContent) {
				t.Fatalf("Content mismatch: expected %q, got %q", string(expectedContent), string(readData))
			}
		})
	}
}

func TestFileWriteLargeData(t *testing.T) {
	largeData := genFixedBytes(uint(fromMebibytes(16))) // 16 MB

	for creatorName, creator := range testCreators {
		t.Run(creatorName, func(t *testing.T) {
			t.Parallel()

			path := uuid.NewString()
			t.Cleanup(func() {
				os.Remove(path)
			})

			file, err := creator.Create(path)
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			remainData := largeData
			for len(remainData) > 0 {
				size := min(len(remainData), 512)
				bytesWritten, err := file.Write(remainData[:size])
				if err != nil {
					t.Fatalf("Failed to write large data: %v", err)
				}
				if bytesWritten != size {
					t.Fatalf("Expected to write %d bytes, but wrote %d bytes", size, bytesWritten)
				}
				remainData = remainData[size:]
			}

			err = file.Close()
			if err != nil {
				t.Fatalf("Failed to close file after writing: %v", err)
			}

			file, err = creator.Open(path)
			if err != nil {
				t.Fatalf("Failed to open file for reading: %v", err)
			}
			var readData = make([]byte, len(largeData))
			size, err := io.ReadFull(file, readData)
			if err != nil {
				t.Fatalf("Failed to read large data: %v", err)
			}
			err = file.Close()
			if err != nil {
				t.Fatalf("Failed to close file after reading: %v", err)
			}
			if size != len(largeData) {
				t.Fatalf("Expected to read %d bytes, but read %d bytes", len(largeData), len(readData))
			}
			if string(readData) != string(largeData) {
				t.Fatalf("Data mismatch: expected %q, got %q", string(largeData), string(readData))
			}
		})
	}
}
