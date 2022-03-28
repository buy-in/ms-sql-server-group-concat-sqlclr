MSBuild.exe GroupConcat.sqlproj /property:Configuration=Release /property:Platform=AnyCPU


#[convert]::ToBase64String((Get-Content -path "GroupConcat/bin/Release/GroupConcat.dll" -Encoding byte))


$bufferSize = 65536
$dll = $pwd.Path + "\GroupConcat\bin\Release\GroupConcat.dll"
$stream = [System.IO.File]::OpenRead($dll)
$r = new-object System.Text.StringBuilder
while ( $stream.Position -lt $stream.Length ) {
#BEGIN CALLOUT A
  $buffer = new-object Byte[] $bufferSize
  $bytesRead = $stream.Read($buffer, 0, $bufferSize)
#END CALLOUT A
  for ($line = 0; $line -lt [Math]::Floor($bytesRead / 16); $line++) {
    $slice = $buffer[($line * 16)..(($line * 16) + 15)]
    $r.AppendFormat("{0:X2}{1:X2}{2:X2}{3:X2}{4:X2}{5:X2}{6:X2}{7:X2}{8:X2}{9:X2}{10:X2}{11:X2}{12:X2}{13:X2}{14:X2}{15:X2}", $slice)
  }
#BEGIN CALLOUT B
  if ($bytesRead % 16 -ne 0) {
    $slice = $buffer[($line * 16)..($bytesRead - 1)]
    foreach ($byte in $slice) {
      $r.AppendFormat("{0:X2}", $byte)
    }
#END CALLOUT B
  }
}
$stream.Close() 
$r.toString()
