package com.mylnikov.hadoophw;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Main.class, FileUtils.class})
public class MainTest {

    private Main main = new Main();

    @Mock
    private HdfsController hdfsController;

    @Mock
    private FileStatus mockDirectory;

    @Mock
    private FileUtils fileUtils;

    @Mock
    private FileStatus mockFile;

    @Mock
    private ParquetConverter mockParquetConverter;

    @Test
    public void shouldSuccessfullyInitController() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.whenNew(HdfsController.class).withAnyArguments().thenReturn(hdfsController);
        main.main(new String[0]);
    }

    @Test
    public void shouldSuccessfullyInitControllerAndParseFiles() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.whenNew(HdfsController.class).withAnyArguments().thenReturn(hdfsController);
        FileStatus[] mockFiles = new FileStatus[2];
        mockFiles[0] = mockDirectory;
        mockFiles[1] = mockFile;
        Mockito.when(mockDirectory.isDirectory()).thenReturn(true);
        Mockito.when(mockFile.getPath()).thenReturn(new Path("test.csv"));
        PowerMockito.mockStatic(FileUtils.class);
        Mockito.when(hdfsController.getFiles(Mockito.any(String.class))).thenReturn(mockFiles);
        PowerMockito.whenNew(ParquetConverter.class).withAnyArguments().thenReturn(mockParquetConverter);
        main.main(new String[0]);
        Mockito.verify(mockParquetConverter, Mockito.times(1)).fromCSV(Mockito.any());
    }

    @Test(expected = IOException.class)
    public void shouldHandleException() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.whenNew(HdfsController.class).withAnyArguments().thenReturn(hdfsController);
        Mockito.when(hdfsController.getFiles(Mockito.any(String.class))).thenThrow(new IOException("Test message"));
        PowerMockito.whenNew(ParquetConverter.class).withAnyArguments().thenReturn(mockParquetConverter);
        main.main(new String[0]);
    }

}

