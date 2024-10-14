import concurrent.futures
from scapy.all import PcapReader
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv2D, MaxPooling2D, Flatten, Dense


#This function is used to parse chunks from pcap and accelerate reading


def parse_pcap_chunk(file_path, start, end):
    headers = []
    with PcapReader(file_path) as pcap_reader:
        for i, packet in enumerate(pcap_reader):
            if i >= start and i < end:
                # Convert packet header to raw bytes (without payload)
                headers.append(bytes(packet))
            if i >= end:
                break
    return headers

def multi_threaded_pcap_parsing(file_path, num_threads=16, chunk_size=100000):
    total_packets = sum(1 for _ in PcapReader(file_path))  # Count total packets
    chunk_ranges = [(i, min(i + chunk_size, total_packets)) for i in range(0, total_packets, chunk_size)]
    
    raw_headers = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(parse_pcap_chunk, file_path, start, end) for start, end in chunk_ranges]
        for future in concurrent.futures.as_completed(futures):
            raw_headers.extend(future.result())  # collective join
    
    raw_data = b''.join(raw_headers)
    return raw_data

#fixed window of 1000
def segment_into_windows(byte_data, window_size=1000):
    num_windows = (len(byte_data) + window_size - 1) // window_size  # ceiling division

    #if not full pad with 0
    padded_data = byte_data.ljust(num_windows * window_size, b'\x00')  # Pad if necessary
    windows = [padded_data[i:i+window_size] for i in range(0, len(padded_data), window_size)]
    return np.array(windows)


def windows_to_2d_matrices(windows, matrix_size=(32, 32)):
    byte_per_matrix = matrix_size[0] * matrix_size[1]  # 32x32=1024
    reshaped_matrices = []
    
    for window in windows:
        padded_window = window.ljust(byte_per_matrix, b'\x00')  # Pad each window if necessary
        matrix = list(padded_window[:byte_per_matrix])  # Convert window to list of integers
        reshaped_matrix = np.array(matrix).reshape(matrix_size)  # Reshape to 2D matrix
        reshaped_matrices.append(reshaped_matrix)
    
    return np.array(reshaped_matrices)

# Step 4: Preprocess the 2D matrices for CNN input
def preprocess_matrices_for_cnn(matrices):
    normalized_matrices = matrices / 255.0  # Normalize byte values (0-255) to the range 0-1
    reshaped_data = normalized_matrices.reshape(len(normalized_matrices), matrices.shape[1], matrices.shape[2], 1)  # Add channel dimension
    return reshaped_data

#model phase
def create_cnn_model(input_shape):
    model = Sequential()
    
    # First convolutional layer
    model.add(Conv2D(32, kernel_size=(3, 3), activation='relu', input_shape=input_shape))
    model.add(MaxPooling2D(pool_size=(2, 2)))
    
    # Second convolutional layer
    model.add(Conv2D(64, kernel_size=(3, 3), activation='relu'))
    model.add(MaxPooling2D(pool_size=(2, 2)))
    
    # Flatten the 2D features
    model.add(Flatten())
    
    model.add(Dense(128, activation='relu'))
    
    # Output layer for binary classification (streaming vs. file transfer)
    model.add(Dense(1, activation='sigmoid'))  # sigmoid is best for binary classification
    
    # Compile the model
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    
    return model

# Step 6: Putting Everything Together
pcap_file = '/tank/swlarsen/2024_07_29/gcc/unzipped/2024_07_29_merged_271.pcapng'

# Multi-threaded parsing to get raw data from headers
raw_data = multi_threaded_pcap_parsing(pcap_file, num_threads=4)

window_size = 1000
windows = segment_into_windows(raw_data, window_size)

matrix_size = (32, 32)
data_matrices = windows_to_2d_matrices(windows, matrix_size)

data_for_cnn = preprocess_matrices_for_cnn(data_matrices)

input_shape = (32, 32, 1)  # Input shape: 32x32 matrices with 
model = create_cnn_model(input_shape)

# Example labels (replace with actual labels: 1 for streaming, 0 for file transfer)
labels = np.array([1] * len(data_for_cnn))  # Example placeholder labels

# Train the CNN model
model.fit(data_for_cnn, labels, batch_size=32, epochs=10, validation_split=0.2)


