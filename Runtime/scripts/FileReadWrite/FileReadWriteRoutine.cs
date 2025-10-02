#define SingleThread // for WebGL platform
using System;
using System.Collections;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using TGL.FileUtility.Data;
using TGL.FileUtility.Info;
using UnityEngine;
#if SingleThread
using System.Linq;
#endif

namespace TGL.FileRW.FileReadWrite
{
    public class FileReadWriteRoutine : FileReadWriteSingleFrame
    {
        #region MonoBehaviourVariables
        /// <summary>
        /// Turns true when we are quitting the app as a MonoBehaviour
        /// </summary>
        protected bool appIsQuitting;
#if SingleThread
        /// <summary>
        /// coroutine pauses to avoid over-working the system in a single frame
        /// </summary>
        protected const int ReturnAfterNCycles = 4;
#endif
        #endregion MonoBehaviourVariables
        
        #region ReadWriteRoutine
        public virtual IEnumerator SaveDataRoutine<T>(T data, string completePath) => SaveDataRoutine(data, completePath, _useMemory);
        public virtual IEnumerator SaveDataRoutine<T>(T data, string completePath, bool useMemory) => SaveDataRoutine(data, new PathDetailed(completePath), useMemory);
        public virtual IEnumerator SaveDataRoutine<T>(T data, PathDetailed detailedPath) => SaveDataRoutine(data, detailedPath, _useMemory);
        public virtual IEnumerator SaveDataRoutine<T>(T data, PathDetailed detailedPath, bool saveInMemory)
        {
	        if(!GetActualPaths(detailedPath, out string completePath, out string completeDirectoryPath))
	        {
		        Debug.LogWarning($"The provided path({detailedPath.fullPath}) was not valid");
		        yield break;
	        }

	        string stringData = null;
	        try
	        {
		        stringData = GetDataAsString(data);
	        }
	        catch (Exception e)
	        {
		        Console.WriteLine(e);
		        throw;
	        }
	        if (string.IsNullOrEmpty(stringData))
	        {
		        // stringData is null or empty, so we can create empty file
		        WriteFileInOneFrame(completePath);
	        }
	        else
	        {
		        bool validForChunks = CalculateStringMemoryUsage(stringData) > MinFileSizeForChunks;
		        if (saveInMemory && canUseMemory)
		        {
			        SaveDataToMemorySingleFrame(completePath, validForChunks, stringData);
		        }
		        // filesBeingWritten.Add(completePath);
		        yield return WriteFileInChunksRoutine(stringData, completePath);
		        // filesBeingWritten.Remove(completePath);
	        }
        }

        public virtual IEnumerator ReadDataRoutine<T>(string completePath, Action<ReadFileData<T>> result) => ReadDataRoutine(completePath, result, _useMemory);
        public virtual IEnumerator ReadDataRoutine<T>(string completePath, Action<ReadFileData<T>> result, bool useMemory) => ReadDataRoutine<T>(new PathDetailed(completePath), result, useMemory);
        public virtual IEnumerator ReadDataRoutine<T>(PathDetailed detailedPath, Action<ReadFileData<T>> result) => ReadDataRoutine<T>(detailedPath, result, _useMemory);
        public virtual IEnumerator ReadDataRoutine<T>(PathDetailed detailedPath, Action<ReadFileData<T>> result, bool readFromMemory)
        {
	        ReadFileData<T> readResult = new ReadFileData<T>();
	        if (!GetActualPaths(detailedPath, out string completePath))
	        {
		        Debug.LogWarning($"The provided path({detailedPath.fullPath}) was not valid");
		        readResult.failureCode = FireReadErrorCodes.PathNotValid;
		        readResult.failureMessage = $"The provided path({detailedPath.fullPath}) was not valid";
		        result?.Invoke(readResult);
		        yield break;
	        }
	        
	        string fileContent = null;
	        if (canUseMemory && readFromMemory)
	        {
		        fileContent = GetDataFromMemorySingleFrame(completePath, out FileInfoData bufferFileData) ? bufferFileData.GetFileData() : string.Empty;
	        }

	        if (string.IsNullOrEmpty(fileContent))
	        {
		        if (!File.Exists(completePath))
		        {
			        readResult.failureCode = FireReadErrorCodes.FileDoesNotExists;
			        readResult.failureMessage = $"File does not exist: {completePath}";
			        result?.Invoke(readResult);
			        yield break;
		        }

		        yield return ReadFileInChunksRoutine(completePath, (content) => fileContent = content);
	        }
	        
	        // Handle empty content
	        if (string.IsNullOrEmpty(fileContent))
	        {
		        readResult.fileData = default;
		        readResult.isReadSuccessfully = true;
		        readResult.failureCode = FireReadErrorCodes.FileContentIsEmpty;
		        readResult.failureMessage = $"File content is null or empty: {completePath}";
		        result?.Invoke(readResult);
		        yield break;
	        }
	        
	        // Handle actual content
	        if (typeof(T) == typeof(string))
	        {
		        // send string as it is, need not deserialize it
		        readResult.fileData = (T)(object)fileContent;
		        readResult.isReadSuccessfully = true;
		        readResult.failureCode = FireReadErrorCodes.None;
		        readResult.failureMessage = null;
		        result?.Invoke(readResult);
	        }
	        else
	        {
		        try
		        {
			        T dataObj = JsonConvert.DeserializeObject<T>(fileContent);
			        readResult.fileData = dataObj;
			        readResult.isReadSuccessfully = true;
			        readResult.failureCode = FireReadErrorCodes.None;
			        readResult.failureMessage = null;
			        result?.Invoke(readResult);
		        }
		        catch (Exception deserializationException)
		        {
			        readResult.failureCode = FireReadErrorCodes.WrongFileDatatype;
			        readResult.failureMessage = $"{completePath} :: Deserialization failed: {deserializationException.Message}";
			        result?.Invoke(readResult);
			        Debug.LogWarning($"{completePath} :: Deserialization failed: {deserializationException.Message}");
		        }
	        }
        }
        #endregion ReadWriteRoutine
        
        #region ReadWriteRoutineImplementation
        private IEnumerator WriteFileInChunksRoutine(string stringData, string completePath)
        {
	        if (string.IsNullOrEmpty(stringData))
	        {
		        WriteFileInOneFrame(completePath);
	        }
	        else
	        {
		        if (CalculateStringMemoryUsage(stringData) > MinFileSizeForChunks)
		        {
			        int totalChunks = (stringData.Length + DefaultBufferSize - 1) / DefaultBufferSize;
			        using FileStream fileStream = new FileStream(completePath, FileMode.Create, FileAccess.Write, FileShare.None, DefaultBufferSize);
			        for (int i = 0; i < totalChunks; i++)
			        {
				        int startIndex = i * DefaultBufferSize;
				        int length = Math.Min(DefaultBufferSize, stringData.Length - startIndex);
				        string chunkStr = stringData.Substring(startIndex, length);

				        // Convert to bytes and write synchronously
				        byte[] bytes = Encoding.UTF8.GetBytes(chunkStr);
				        fileStream.Write(bytes, 0, bytes.Length);

				        // Yield every few chunks to maintain frame responsiveness
				        if (i % ReturnAfterNCycles == 0)
				        {
					        yield return null;
				        }

				        if (appIsQuitting)
				        {
					        break;
				        }
			        }
			        fileStream.Flush(); // Ensure all data is written
		        }
		        else
		        {
			        WriteFileInOneFrame(completePath, stringData);
		        }
	        }
        }

        private IEnumerator ReadFileInChunksRoutine(string completePath, Action<string> fileContent)
        {
	        if (!File.Exists(completePath))
	        {
		        fileContent?.Invoke(string.Empty);
	        }

	        FileInfo fileInfo = new FileInfo(completePath);
	        if (fileInfo.Length > MinFileSizeForChunks)
	        {
		        int bufferSize = Math.Min((int)fileInfo.Length, DefaultBufferSize);
		        using FileStream fileStream = new FileStream(completePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize);
		        byte[] readBuffer = new byte[DefaultBufferSize];
		        StringBuilder contentBuilder = new StringBuilder();
		        int bytesRead;
		        int chunkCount = 0;
		        do
		        {
			        bytesRead = fileStream.Read(readBuffer, 0, readBuffer.Length);
			        if (bytesRead > 0)
			        {
				        string chunk = Encoding.UTF8.GetString(readBuffer, 0, bytesRead);
				        contentBuilder.Append(chunk);
				        chunkCount++;
				        // Yield every few chunks to maintain frame responsiveness
				        if (chunkCount % ReturnAfterNCycles == 0)
				        {
					        yield return null;
				        }
				        
				        if (appIsQuitting)
				        {
					        break;
				        }
			        }
		        } while (bytesRead > 0);

		        fileContent?.Invoke(contentBuilder.ToString());
	        }
	        else
	        {
		        fileContent?.Invoke(ReadExistingFileInOneFrame(completePath));
	        }
        }
        #endregion ReadWriteRoutineImplementation

        #region MonoBehaviourMethods
        private void Awake()
        {
	        Application.quitting += UpdateAppQuitting;
        }

        private void OnDestroy()
        {
	        Application.quitting -= UpdateAppQuitting;
        }
        #endregion MonoBehaviourMethods

        #region AppManagement
        private void UpdateAppQuitting()
        {
	        appIsQuitting = true;
        }
        #endregion AppManagement
    }
}