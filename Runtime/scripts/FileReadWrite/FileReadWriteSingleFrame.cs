using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using TGL.FileUtility.Data;
using TGL.FileUtility.Info;
using TGL.FileUtility.Utility;
using UnityEngine;

namespace TGL.FileRW.FileReadWrite
{
	public class FileReadWriteSingleFrame : MonoBehaviour
	{
		#region FilesStoredInMemoryVariables
		/// <summary>
		/// How many files are we allowed to store? 
		/// </summary>
		protected int numOfFilesToStore;
		/// <summary>
		/// are the conditions right for using memory
		/// </summary>
		protected bool canUseMemory;
		/// <summary>
		/// Do we store files in memory to be easily accessible?<br/>
		/// if we use this, our app will use more Memory but less processing power
		/// </summary>
		protected bool _useMemory;
		/// <summary>
		/// file cache, we can ask to save the most frequently updated files here 
		/// </summary>
		protected ConcurrentDictionary<string, FileInfoData> fileStoredBuffer;
		/// <summary>
		/// When was a given file path accessed last? The higher the Value of the Dictionary, the more recent the access
		/// </summary>
		protected ConcurrentDictionary<string, int> fileAccessDictionary;
		/// <summary>
		/// The count which increases every time a file is read or written to
		/// </summary>
		protected int fileAccessCount;
		/// <summary>
		/// Minimum file size that uses chunks, files below this size will be directly written (default = 80 KB)
		/// </summary>
		protected const int MinFileSizeForChunks = 81920; // 80 KB
		/// <summary>
		/// Average file size we use to check if memory used is balanced (default = 10 MB)
		/// </summary>
		protected const int MaxMemoryFileSizeAvg = 10485760; // 10 MB (10 * 1024 * 1024)
		#endregion FilesStoredInMemoryVariables
		
		#region constantVariables
		/// <summary>
		/// We should never keep buffer size below 128 bytes in any platform, as it is less than 30 characters in a file
		/// </summary>
		protected const int MinBufferSize = 128; // 128 Bytes 
		/// <summary>
		/// The buffer size this code will use throughout the code
		/// </summary>
		protected const int DefaultBufferSize = 16384; // 16 KB 
		// other values in case default buffer size needs to be changed.
		protected const int Chunk4Kb =   4096; // 4 KB
		protected const int Chunk8Kb =   8192; // 8 KB
		protected const int Chunk16Kb = 16384; // 16 KB
		protected const int Chunk80Kb = 81920; // 80 KB
		#endregion constantVariables
		
		#region InitializeRegion
		/// <summary>
		/// Initializes the system to be used
		/// </summary>
		/// <param name="useWriteBuffer">in this instance, always false, child classes may override this behaviour</param>
		/// <param name="useMemoryByDefault">should we keep a copy in memory for faster access (by default) for all files</param>
		/// <param name="maxStoredFileCount">How many files are max to be stored in memory</param>
		public virtual void Initialize(bool useWriteBuffer = false, bool useMemoryByDefault = false, int maxStoredFileCount = 0)
		{
			canUseMemory = maxStoredFileCount > 0;
			if (canUseMemory)
			{
				numOfFilesToStore = maxStoredFileCount;
				fileStoredBuffer ??= new ConcurrentDictionary<string, FileInfoData>();
				fileAccessDictionary ??= new ConcurrentDictionary<string, int>();
				_useMemory = useMemoryByDefault;
			}
			else
			{
				numOfFilesToStore = 0;
				fileStoredBuffer?.Clear();
				fileAccessDictionary?.Clear();
				_useMemory = false;
			}
		}
		#endregion InitializeRegion
		
		
		#region sameFrameReadWrite
		public virtual bool SaveData<T>(T data, string completePath) => SaveData(data, new PathDetailed(completePath), _useMemory);
		public virtual bool SaveData<T>(T data, string completePath, bool useMemory) => SaveData(data, new PathDetailed(completePath), useMemory);
		public virtual bool SaveData<T>(T data, PathDetailed detailedPath) => SaveData(data, detailedPath, _useMemory);
		public virtual bool SaveData<T>(T data, PathDetailed detailedPath, bool saveInMemory)
		{
			if(!GetActualPaths(detailedPath, out string completePath, out string completeDirectoryPath))
			{
				Debug.LogWarning($"The provided path({detailedPath.fullPath}) was not valid");
				return false;
			}
			
			try
			{
				string stringData = GetDataAsString(data);
				if (string.IsNullOrEmpty(stringData))
				{
					// stringData is null or empty, so we can create / replace the file
					WriteFileInOneFrame(completePath);
				}
				else
				{
					if (canUseMemory && saveInMemory)
					{
						SaveDataToMemorySingleFrame(completePath, false, stringData);
					}
					
					// Use FileStream with buffer for better performance in writing
					WriteFileInOneFrame(completePath, stringData);
				}

				// return file saved successfully
				return true;
			}
			catch (IOException ioExp)
			{
				Debug.LogWarning($"An IO exception occured trying to write the file '{detailedPath.fullPath}' :: " + ioExp.Message);
				return false;
			}
			catch (Exception exp)
			{
				Debug.LogWarning($"An exception occured trying to write the file '{detailedPath.fullPath}' :: "+ exp.Message);
				return false;
			}
		}
		
		public virtual T ReadData<T>(string completePath, out string failureMsg) => ReadData<T>(new PathDetailed(completePath), out failureMsg, _useMemory);
		public virtual T ReadData<T>(string completePath, out string failureMsg, bool useMemory) => ReadData<T>(new PathDetailed(completePath), out failureMsg, useMemory);
		public virtual T ReadData<T>(PathDetailed detailedPath, out string failureMsg) => ReadData<T>(detailedPath, out failureMsg, _useMemory);
		public virtual T ReadData<T>(PathDetailed detailedPath, out string failureMsg, bool readFromMemory)
		{
			failureMsg = null;
			if (!GetActualPaths(detailedPath, out string completePath))
			{
				Debug.LogWarning($"The provided path({detailedPath.fullPath}) was not valid");
				return default;
			}
			
			try
			{
				string fileContent = null;
				if (canUseMemory && readFromMemory)
				{
					fileContent = GetDataFromMemorySingleFrame(completePath, out FileInfoData bufferFileData) ? bufferFileData.GetFileData() : string.Empty;
				}
				
				if(string.IsNullOrEmpty(fileContent))
				{
					if (!File.Exists(completePath))
					{
						failureMsg = $"File does not exist: {completePath}";
						return default;
					}
					fileContent = ReadExistingFileInOneFrame(completePath);
				}
				
				// Handle empty content
				if (string.IsNullOrEmpty(fileContent))
				{
					failureMsg = $"File content is null or empty: {completePath}";
					if (typeof(T) == typeof(string))
					{
						return (T)(object)string.Empty;
					}
					return default;
				}
				
				// Return appropriate type
				if (typeof(T) == typeof(string))
				{
					return (T)(object)fileContent;
				}
				else
				{
					try
					{
						return JsonConvert.DeserializeObject<T>(fileContent);
					}
					catch (Exception deserializationException)
					{
						failureMsg = $"{completePath} :: Deserialization failed: {deserializationException.Message}";
						Debug.LogWarning($"{completePath} :: Deserialization failed: {deserializationException.Message}");
						return default;
					}
				}
			}
			catch (IOException ioExp)
			{
				Debug.LogWarning($"An IO exception occured trying to read the file '{completePath}' :: "+ ioExp.Message);
				failureMsg = $"{completePath} :: {ioExp.Message}";
				return default;
			}
			catch (Exception exp)
			{
				Debug.LogWarning($"An exception occured trying to read the file '{completePath}' :: "+ exp.Message);
				failureMsg = $"{completePath} :: {exp.Message}";
				return default;
			}
		}

		#endregion sameFrameReadWrite
		
		#region StringProcessing
		/// <summary>
		/// This calculates exact memory used <br/>
		/// if we don't want such detailed usage, we can use `System.Text.Encoding.UTF8.GetByteCount(string)`
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		protected static int CalculateStringMemoryUsage(string str)
		{
			if (str == null) return 0;
    
			// Base object overhead (approx 20 bytes in 32-bit, 28-30 in 64-bit)
			int overhead = IntPtr.Size * 3; // Approximation for object header + method table pointer
    
			// String length field (4 bytes)
			int lengthField = 4;
    
			// Character data (2 bytes per character in UTF-16)
			int characterData = str.Length * 2;
    
			// Null terminator (2 bytes)
			int nullTerminator = 2;
    
			return overhead + lengthField + characterData + nullTerminator;
		}

		protected static bool GetActualPaths(PathDetailed relativePath, out string completePath, out string completeDirectoryPath)
		{
			completePath = Path.Combine(Application.persistentDataPath, relativePath.fullPath);
			completeDirectoryPath = Path.Combine(Application.persistentDataPath, relativePath.directoryPath);
			Directory.CreateDirectory(completeDirectoryPath); // creates Directory if it does not exists.
			return relativePath.IsValid();
		}
		
		protected static bool GetActualPaths(PathDetailed relativePath, out string completePath)
		{
			completePath = Path.Combine(Application.persistentDataPath, relativePath.fullPath);
			return relativePath.IsValid();
		}

		protected static string GetDataAsString<T>(T data)
		{
			string stringData;
			if (typeof(T) == typeof(string))
			{
				stringData = data as string;
			}
			else
			{
				//com.unity.nuget.newtonsoft-json
				try
				{
					stringData = JsonConvert.SerializeObject(data);
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
					throw;
				}
			}
			return stringData;
		}
		#endregion StringProcessing
		
		#region MemoryProcessing
		/// <summary>
		/// removes file data from <see cref="fileStoredBuffer"/> buffer, as memory is being used too much
		/// </summary>
		protected void CheckMemoryDataNeedsCleaning()
		{
			if (fileStoredBuffer is not { Count: > 0 })
			{
				return;
			}

			if (fileStoredBuffer.Count < numOfFilesToStore)
			{
				return;
			}

			int totalMemorySize = 0;
			PriorityQueue<FileInfoData, int> storedFileOrdered = new PriorityQueue<FileInfoData, int>(); // Available in .Net 6+, using custom as it is not available in current Unity version
			foreach (KeyValuePair<string, FileInfoData> storedBufferFile in fileStoredBuffer)
			{
				totalMemorySize += CalculateStringMemoryUsage(storedBufferFile.Value.GetFileData());
				if (fileAccessDictionary.TryGetValue(storedBufferFile.Value.completeFilePath, out int accessNumber))
				{
					storedFileOrdered.Enqueue(storedBufferFile.Value, accessNumber);
				}
			}

			while (totalMemorySize > MaxMemoryFileSizeAvg * numOfFilesToStore)
			{
				// Delete oldest accessed file
				FileInfoData oldestData = storedFileOrdered.Dequeue();
				if (fileStoredBuffer.TryRemove(oldestData.completeFilePath, out oldestData))
				{
					totalMemorySize -= CalculateStringMemoryUsage(oldestData.GetFileData());
				}
			}
		}

		/// <summary>
		/// Saves the file data in the memory
		/// </summary>
		/// <param name="completePath">path of the file</param>
		/// <param name="validForChunks">can we use chunks to read and write</param>
		/// <param name="stringData">the data in the file</param>
		protected void SaveDataToMemorySingleFrame(string completePath, bool validForChunks, string stringData)
		{
			FileInfoData writeData = new FileInfoData(completePath,false, true, validForChunks);
			writeData.UpdateFileData(stringData);
			fileAccessDictionary[completePath] = fileAccessCount++;
			fileStoredBuffer[completePath] = writeData;
			CheckMemoryDataNeedsCleaning();
		}

		/// <summary>
		/// Reads the memory files for data about file at <paramref name="completePath"/>
		/// </summary>
		/// <param name="completePath">the complete path of the file</param>
		/// <param name="bufferFileData">the data from buffer memory</param>
		/// <returns>did we find the data?</returns>
		protected bool GetDataFromMemorySingleFrame(string completePath, out FileInfoData bufferFileData)
		{
			if (fileStoredBuffer.TryGetValue(completePath, out bufferFileData))
			{
				fileAccessDictionary[completePath] = fileAccessCount++;
				return true;
			}
			return false;
		}
		#endregion MemoryProcessing

		#region FileIO
		protected string ReadExistingFileInOneFrame(string completePath)
		{
			FileInfo fileInfo = new FileInfo(completePath);
			int bufferSize = Math.Min((int)fileInfo.Length, DefaultBufferSize);
			using FileStream stream = new FileStream(completePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan);
			using StreamReader reader = new StreamReader(stream, Encoding.UTF8);
			return reader.ReadToEnd();
		}

		protected void WriteFileInOneFrame(string completePath, string stringData)
		{
			using FileStream stream = new FileStream(completePath, FileMode.Create, FileAccess.Write, FileShare.None, DefaultBufferSize, FileOptions.SequentialScan); // 16kb, Sequential scan
			byte[] bytes = Encoding.UTF8.GetBytes(stringData);
			stream.Write(bytes, 0, bytes.Length);
			stream.Flush(); // Explicitly flush any buffered data (though Dispose does this anyway)
		}
		
		protected void WriteFileInOneFrame(string completePath)
		{
			using FileStream stream = new FileStream(completePath, FileMode.Create, FileAccess.Write, FileShare.None, MinBufferSize, FileOptions.SequentialScan); // 128, Sequential scan
		}
		#endregion FileIO
	}
}