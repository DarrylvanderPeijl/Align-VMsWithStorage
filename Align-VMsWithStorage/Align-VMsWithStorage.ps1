<#
	.DESCRIPTION
    
		Align-VMsWithStorage is a script that can be used to align your VMs
        with your Cluster Shared Volumes. With Storage Spaces Direct all writes
        are routed through the owner of the CSV which can cause ineffiecient IO paths.
        This script makes sure your VM is running on the same host which owns the CSV
        the VM is running on.
        
        The script finds the volume where most of your disks of your VMs are on and moves the VM there.
        If you have equally spread disks, it will use the first disk as target.
                
	.INPUTS
 
		None
 
	.OUTPUTS
 
		None
 
	.NOTES
 
		Author: Darryl van der Peijl
		Website: http://www.DarrylvanderPeijl.nl/
		Email: DarrylvanderPeijl@outlook.com
		Date created: 31.december.2018
		Last modified: 17.january.2019
		Version: 0.9.3

 
	.LINK
    
		http://www.DarrylvanderPeijl.nl/
		https://twitter.com/DarrylvdPeijl
#>

    [CmdletBinding(SupportsShouldProcess = $true,ConfirmImpact='High')] 
    param (
        [alias("CimSession ")]
        [Parameter(Position = 0)][String]$Clustername = "localhost",
        [switch]$QuickMigration,
        [int]$NodePhysicalmemorybufferGB = 8

    )
    begin{
        #region:Helper Functions
        Function GetCSVIDbyPath {
            param ($CSVpath)

            Begin {
                Write-Verbose -Message "Trying to find CSV ID with path $CSVpath"
            }
            Process {
                $CSVhash.keys | ForEach-Object {
            
                    #If value found, write out key
                    If ($CSVhash[$_] -like $CSVpath) {
                        Write-Verbose -Message "Found ID for $CSVPath - $_"
                        Write-output $_
                    }
                }
            }
        }
        #endregion

        #region GatherInfoNeeded

        #Cluster
        $Cluster = Get-Cluster -Name $Clustername
        $Clustername = $Cluster.Name + "." + $cluster.Domain

        if (!($Cluster)) {
            Write-Error "Could not find Cluster with name $Clustername"
        }
        Else {
            $domain = $Cluster.Domain
            Write-Verbose "Found Cluster $Clustername in domain $domain"
        }

        #ClusterNodes
        $clusternodes = Get-ClusterNode -cluster $Clustername
        if (!($clusternodes)) {
            Write-Error "Could not find cluster nodes in cluster $Clustername"
        }
        Else {
            $count = ($clusternodes).count
            Write-Verbose "Found $count cluster nodes in cluster $Clustername"
        }

        #ClusterSharedVolumes
        Write-Verbose "Getting CSV(s) from $Clustername"
        $Clustersharedvolumes = Get-ClusterSharedVolume -Cluster $Clustername

        if (!($Clustersharedvolumes)) {
            Write-Error "Could not find Cluster Shared Volumes on $Clustername"
        }
        Else {
            $count = ($Clustersharedvolumes).count
            Write-Verbose "Found $count CSV(s) on cluster $Clustername"
        }




        #endregion
    }
    process{
        #region interpret information

        $CSVhash = @{}
        Foreach ($Clustersharedvolume in $Clustersharedvolumes) {
            $Matches = $null
            #Extract the CSV volume names from cluster resource name
            $Null = $Clustersharedvolume.name -match ".*?\((.*?)\)" 
            if ($Matches) {
                $CSVname = $($Matches[1])
                Write-Verbose -Message "Regex matched and found $CSVname"

            }else {
                Write-Verbose -Message "Regex did not match, probably renamed CSV"
                $CSVname = $Clustersharedvolume.name
                $FirstClusternode = $clusternodes[0].Name

                Write-Verbose -Message "Checking if virtualdisk with name $CSVname exist on $FirstClusternode"
                If ((Get-VirtualDisk $CSVname -CimSession $FirstClusternode))
                    {
                    Write-Verbose -Message "virtualdisk with name $CSVname exist on $FirstClusternode"
                }else{
                 Throw "Cannot find CSV with name $CSVname, something is wrong. Exitting."
                 Exit
                }
                 

            }

            Write-Verbose -Message "Gathering information of CSV $CSVname"

            $CSVhash[($Clustersharedvolume.Id).ToString()] = @($CSVname, $Clustersharedvolume.OwnerNode.Name, $Clustersharedvolume.SharedVolumeInfo.FriendlyVolumeName)
        }



        ## Find VMs and the CSV they live on and the host they live on
        ## exclude VMs with disks on multiple CSVs (for now?)

        $VMhash = @{}

        Foreach ($clusternode in $clusternodes) {
            $VMs = $null
            [array]$VMs += Get-VM -ComputerName $clusternode
        }
        Foreach ($VM in $VMs) {
        
            Write-Verbose -Message "Finding disks for $($VM.name)"
        
            $CSVtemparray = @()
            Foreach ($disk in ($vm | Get-VMHardDiskDrive)) {
            
                $diskpathsplit = $disk.path -split '\\' 
                $CSVpath = $diskpathsplit[0] + "\" + $diskpathsplit[1] + "\" + $diskpathsplit[2]
                Write-Verbose -Message "Found disk on $CSVpath"
                Write-Verbose -Message "Trying to find CSV ID through GetCSVIDbyPath function"
                $TempCSVID = GetCSVIDbyPath -CSVpath $CSVpath

                $CSVtemparray += $TempCSVID
            
            }

            $CSVID = ($CSVtemparray | Group-Object | Sort-Object Count -descending | Select-Object -First 1).name


            Write-Verbose -Message "Adding $($VM.Name) to hashtable using CSV ID $CSVID"
            $vmid = $VM.VMID.ToString()
            $VMhash[$vmid] = @($VM.name, $CSVID, $VM.ComputerName, $VM.MemoryAssigned, $VM.State)
        
        }

        ##Create task list of volumes to move to be in optimal condition
        $VMstoMove = @{}
        #Check if VM is on same disk as node
        $VMhash.Keys | ForEach-Object {

            $VMOwner = $VMhash[$_][2]
            $CSVOwner = $CSVhash[$VMhash[$_][1]][1]

            if ($VMOwner -eq $CSVOwner) {
                Write-Verbose -Message "VM with ID $_ is on same host as CSV"
            
            }
            else {
                Write-Verbose -Message "VM with ID $_ is on different host as CSV and can be optimized"
            
                $vmid = $_.ToString()    
                $VMstoMove[$vmid] = @($VMhash[$_][0], $CSVOwner, $VMhash[$_][3], $VMhash[$_][4])

            }



        }

        #endregion 

        #region Move VMs


        Write-Output "Found $($($VMstoMove).count) VM(s) to be optimized."
        Write-Verbose "Quickmigration switch = $Quickmigration"

        $VMstoMove.Keys | ForEach-Object {

            $vmid = $_.ToString()
            $vmname = $VMstoMove[$_][0]
            $targetnode = $VMstoMove[$_][1]
            $VMstate = $VMstoMove[$_][3]
            $VMmem = $VMstoMove[$_][2] / 1024 / 1024 / 1024

            Write-Verbose -Message "Intent to move $vmname to $targetnode"


            $NodeFreeMem = ((Get-WMIObject Win32_OperatingSystem -computername $targetnode).FreePhysicalMemory / 1024 / 1024)

            Write-Verbose -Message "$targetnode has $NodeFreeMem of free physical memory,$VMname needs $VMmem"

            If (($NodeFreeMem + $NodePhysicalmemorybufferGB) -gt $VMmem) {
                Write-Verbose -Message "$targetnode has enough resources to host $VMname"
                        
                if ($VMstate -eq "Off" -or $Quickmigration -eq $true) {
                    
                    if ($PSCmdlet.ShouldProcess(
                            ("{0}" -f $vmname),
                            ("Migrating to {0} using quick migration" -f $targetnode)
                            
                        )
                    ) {
                        Write-Output "VM $vmname is $VMstate and being moved to $targetnode using quick migration"
                        $MoveAction = Move-ClusterVirtualMachineRole -Cluster $Clustername -VMId $vmid -Node $targetnode -MigrationType Quick
                        If ($MoveAction.OwnerNode -eq $targetnode) {
                            Write-Output "VM $vmname succesfully moved to $targetnode"
                        }
                        Else {

                            Throw "VM $vmname not succesfully moved to $targetnode,exiting!"
                            Exit

                        }
                    }
                }
                ElseIf ($VMstate -eq "Running") {
                    
                    if ($PSCmdlet.ShouldProcess(
                            ("{0}" -f $vmname),
                            ("Migrating to {0} using live migration" -f $targetnode)
                       )
                    ) {
                        Write-Output "VM $vmname is running and being moved to $targetnode using live migration"
                        $MoveAction = Move-ClusterVirtualMachineRole -Cluster $Clustername -VMId $vmid -Node $targetnode
                        If (($MoveAction.OwnerNode -eq $targetnode) -and ($MoveAction.State -eq "Online")) {
                            Write-Output "VM $vmname succesfully moved to $targetnode"
                        }
                        Else {

                            Throw "VM $vmname not succesfully moved to $targetnode,exiting!"
                            Exit

                        }
                    }
                }
                Else {
                    Write-Verbose "Status of VM $vmname is $VMstate"
                    Write-Output "Status of VM $vmname is other then Running or Off, skipping VM."

                }
            }
            Else {
            
                Write-Verbose -Message "Not enough resources for $vmname on $targetnode"
            
            }


        }

        #endregion
    }