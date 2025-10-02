using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TGL.FileUtility.Data;
using TGL.FileUtility.Info;
using UnityEngine;

namespace TGL.FileRW.FileReadWrite
{
	public class FileReadWriteTaskManager : FileReadWriteRoutine
	{
		#region AsyncReadWrite
		public virtual async Task<bool> SaveDataAsync<T>(T data, string path) => await SaveDataAsync(data, path, _useMemory);
		public virtual async Task<bool> SaveDataAsync<T>(T data, string path, bool useMemory) => await SaveDataAsync(data, new PathDetailed(path), useMemory);
		public virtual async Task<bool> SaveDataAsync<T>(T data, PathDetailed detailedPath) => await SaveDataAsync(data, detailedPath, _useMemory);
		public virtual async Task<bool> SaveDataAsync<T>(T data, PathDetailed detailedPath, bool saveInMemory)
		{
			if(!GetActualPaths(detailedPath, out string completePath, out string completeDirectoryPath))
			{
				Debug.LogWarning($"The provided path({detailedPath.fullPath}) was not valid");
				return false;
			}

			try
			{
				Directory.CreateDirectory(completeDirectoryPath); // creates Directory if it does not exists.
				string stringData = GetDataAsString(data);
				if (string.IsNullOrEmpty(stringData))
				{
					// stringData is null or empty, so we can create empty file
					WriteFileInOneFrame(completePath);
					return true;
				}
				else
				{
					bool validForChunks = CalculateStringMemoryUsage(stringData) > MinFileSizeForChunks;
					if (saveInMemory && canUseMemory)
					{
						SaveDataToMemorySingleFrame(completePath, validForChunks, stringData);
					}
					// filesBeingWritten.Add(completePath);
					return await WriteFileInChunksTask(stringData, completePath);
					// filesBeingWritten.Remove(completePath);
				}
			}
			catch (IOException ioExp)
			{
				Debug.LogWarning($"An IO exception occured trying to write the file '{completePath}' :: "+ ioExp.Message);
				return false;
			}
			catch (Exception exp)
			{
				Debug.LogWarning($"An exception occured trying to write the file '{completePath}' :: "+ exp.Message);
				return false;
			}
		}
		
		public virtual async Task ReadDataAsync<T>(string path, Action<ReadFileData<T>> result) => await ReadDataAsync<T>(path, result, _useMemory);
		public virtual async Task ReadDataAsync<T>(string path, Action<ReadFileData<T>> result, bool useMemory) => await ReadDataAsync<T>(new PathDetailed(path), result, useMemory);
		public virtual async Task ReadDataAsync<T>(PathDetailed detailedPath, Action<ReadFileData<T>> result) => await ReadDataAsync<T>(detailedPath, result, _useMemory);
		public virtual async Task ReadDataAsync<T>(PathDetailed detailedPath, Action<ReadFileData<T>> result, bool readFromMemory)
		{
			ReadFileData<T> readResult = new ReadFileData<T>();
			if(!GetActualPaths(detailedPath, out string completePath))
			{
				Debug.LogWarning($"The provided path({detailedPath.fullPath}) was not valid");
				readResult.failureCode = FireReadErrorCodes.PathNotValid;
				readResult.failureMessage = $"The provided path({detailedPath.fullPath}) was not valid";
				result?.Invoke(readResult);
				return;
			}

			try
			{
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
						return;
					}
					fileContent = await File.ReadAllTextAsync(completePath);
				}

				// Handle empty content
				if (string.IsNullOrEmpty(fileContent))
				{
					readResult.fileData = default;
					readResult.isReadSuccessfully = true;
					readResult.failureCode = FireReadErrorCodes.FileContentIsEmpty;
					readResult.failureMessage = $"File content is null or empty: {completePath}";
					result?.Invoke(readResult);
					return;
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
					return;
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
						return;
					}
					catch (Exception deserializationException)
					{
						readResult.failureCode = FireReadErrorCodes.WrongFileDatatype;
						readResult.failureMessage = $"{completePath} :: Deserialization failed: {deserializationException.Message}";
						result?.Invoke(readResult);
						return;
						Debug.LogWarning($"{completePath} :: Deserialization failed: {deserializationException.Message}");
					}
				}
			}
			catch (IOException ioExp)
			{
				Debug.LogWarning($"An IO exception occured trying to read the file '{completePath}' :: "+ ioExp.Message);
				readResult.failureCode = FireReadErrorCodes.IOException;
				readResult.failureMessage = $"{completePath} :: {ioExp.Message}";
				result?.Invoke(readResult);
				return;
			}
			catch (Exception exp)
			{
				Debug.LogWarning($"An exception occured trying to read the file '{completePath}' :: "+ exp.Message);
				readResult.failureCode = FireReadErrorCodes.UndefinedException;
				readResult.failureMessage = $"{completePath} :: {exp.Message}";
				result?.Invoke(readResult);
				return;
			}
		}
		#endregion AsyncReadWrite

		#region ReadWriteTaskImplementation
		private async Task<bool> WriteFileInChunksTask(string stringData, string completePath)
		{
			// Return false immediately if the application is quitting or data is null/empty.
			if (appIsQuitting || string.IsNullOrEmpty(stringData))
			{
				if (string.IsNullOrEmpty(stringData))
				{
					WriteFileInOneFrame(completePath); 
					return true;
				}
				return false; // Only returns false if appIsQuitting is true AND data was present (aborting the write)
			}

			try
			{
				if (CalculateStringMemoryUsage(stringData) <= MinFileSizeForChunks)
				{
					WriteFileInOneFrame(completePath);
					return true;
				}
        
				byte[] stringBytes = Encoding.UTF8.GetBytes(stringData);
				int totalLength = stringBytes.Length;
				int totalChunks = (totalLength + DefaultBufferSize - 1) / DefaultBufferSize;
				int bytesWritten = 0;
				await using (var fileStream = new FileStream(completePath, FileMode.Create, FileAccess.Write, FileShare.None, DefaultBufferSize))
				{
					for (int i = 0; i < totalChunks; i++)
					{
						int startIndex = i * DefaultBufferSize;
						int length = Math.Min(DefaultBufferSize, totalLength - startIndex);
						await fileStream.WriteAsync(stringBytes, startIndex, length);
						bytesWritten += length;
					}
					await fileStream.FlushAsync();
				}
				return bytesWritten == totalLength; 
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				throw; 
			}
		}
		#endregion ReadWriteTaskImplementation
	}
} 